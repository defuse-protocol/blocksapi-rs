#[macro_use]
extern crate derive_builder;

use futures::StreamExt;
use std::io::Read;
use tokio::sync::mpsc;

pub mod config;
pub use config::{BlocksApiConfig, BlocksApiConfigBuilder};

// Generated protobuf code from aurora-is-near/borealis-prototypes
// The proto files need to be obtained from: https://github.com/aurora-is-near/borealis-prototypes
#[allow(clippy::all)]
pub mod blocksapi {
    // This will use the generated code from build.rs
    include!(concat!(env!("OUT_DIR"), "/borealis.blocksapi.rs"));
}

#[allow(clippy::all)]
pub mod payloads {
    include!(concat!(env!("OUT_DIR"), "/borealis.payloads.rs"));
    #[allow(clippy::all)]
    pub mod near {
        include!(concat!(env!("OUT_DIR"), "/borealis.payloads.near.rs"));
    }
}

pub(crate) const BLOCKSAPI_RS: &str = "blocksapi-rs";

// Borealis envelope structure
// The struct uses `cbor:",toarray"` which means it's serialized as an array
#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
struct BorealisEnvelope(
    u16,      // type_field
    u64,      // sequential_id
    u32,      // timestamp_s
    u16,      // timestamp_ms
    [u8; 16], // unique_id
);

async fn decode_borealis_payload<T>(data: &[u8]) -> anyhow::Result<T>
where
    T: for<'de> serde::Deserialize<'de>,
{
    if data.is_empty() {
        return Err(anyhow::anyhow!("Empty data"));
    }

    let version = data[0];
    match version {
        1 => {
            let mut reader = std::io::Cursor::new(&data[1..]);

            // Decode the envelope first
            let _envelope: BorealisEnvelope = ciborium::from_reader(&mut reader)?;

            // Then decode the payload
            let payload: T = ciborium::from_reader(&mut reader)?;
            Ok(payload)
        }
        _ => Err(anyhow::anyhow!(
            "Unknown version of borealis-message: {}",
            version
        )),
    }
}

async fn decode_near_block_json(data: &[u8]) -> anyhow::Result<Vec<u8>> {
    // First decode the borealis payload to get the LZ4 compressed data
    let lz4_data: Vec<u8> = decode_borealis_payload(data).await?;
    let mut buf = vec![];
    lz4::Decoder::new(std::io::Cursor::new(lz4_data))?.read_to_end(&mut buf)?;
    Ok(buf)
}

async fn convert_to_streamer_message(
    message: blocksapi::ReceiveBlocksResponse,
) -> anyhow::Result<near_indexer_primitives::StreamerMessage> {
    let msg = match message.response {
        Some(blocksapi::receive_blocks_response::Response::Message(msg)) => match msg.message {
            Some(block_msg) => block_msg,
            None => {
                anyhow::bail!("Received BlockMessage with no message field");
            }
        },
        _ => {
            anyhow::bail!("Received message with no response field");
        }
    };

    // Extract the raw payload
    let raw_payload = if let Some(blocksapi::block_message::Payload::RawPayload(data)) = msg.payload
    {
        data
    } else {
        anyhow::bail!("Unsupported payload type or missing payload");
    };

    // Check the format - we expect PAYLOAD_NEAR_BLOCK_V4 (JSON format)
    match blocksapi::block_message::Format::try_from(msg.format)? {
        blocksapi::block_message::Format::PayloadNearBlockV2 => {
            // For V2, we need to decode the borealis envelope and decompress LZ4
            let json_data = decode_near_block_json(&raw_payload).await?;
            Ok(serde_json::from_slice::<
                near_indexer_primitives::StreamerMessage,
            >(&json_data)?)
        }
        _ => {
            anyhow::bail!("Unsupported block message format: {:?}", msg.format);
        }
    }
}

pub async fn start(
    streamer_message_sink: mpsc::Sender<near_indexer_primitives::StreamerMessage>,
    config: BlocksApiConfig,
) -> anyhow::Result<()> {
    let mut client = config.client().await?;
    let request = config.request().await;
    let metadata = config.metadata().await;
    let mut req = tonic::Request::new(request);
    *req.metadata_mut() = metadata;

    let response: tonic::Response<tonic::Streaming<blocksapi::ReceiveBlocksResponse>> =
        client.receive_blocks(req).await?;
    let mut stream = response.into_inner();

    // Process the message synchronously to maintain block order
    // Collect a batch of messages and process them in parallel
    // Collect messages in batches, process in parallel, and send in order
    let mut batch: Vec<blocksapi::ReceiveBlocksResponse> = Vec::new();
    let mut current_height: u64 = 0;

    loop {
        tokio::select! {
            // Handle incoming messages
            message = stream.next() => {
                match message {
                    Some(Ok(response)) => {

                        // Add current response to batch
                        batch.push(response);

                        // Process batch when it reaches the batch size
                        if batch.len() >= config.batch_size {
                            let batch = std::mem::take(&mut batch);
                            let mut results = Vec::with_capacity(batch.len());

                            // Process in parallel
                            let mut handles = Vec::new();
                            for msg in batch {
                                let handle = tokio::spawn(convert_to_streamer_message(msg));
                                handles.push(handle);
                            }

                            // Collect results
                            for handle in handles {
                                match handle.await {
                                    Ok(Ok(streamer_msg)) => {
                                        results.push(streamer_msg);
                                    },
                                    Ok(Err(e)) => {
                                        tracing::error!(target: BLOCKSAPI_RS, "Error converting message: {}", e);
                                    },
                                    Err(e) => {
                                        tracing::error!(target: BLOCKSAPI_RS, "Task error: {}", e);
                                    }
                                }
                            }

                            // Sort by height to maintain order
                            results.sort_by_key(|msg| msg.block.header.height);

                            // Send in order
                            for streamer_msg in results {
                                let block_height = streamer_msg.block.header.height;

                                // Ensure we're not going backwards
                                if block_height < current_height {
                                    tracing::warn!(
                                        target: BLOCKSAPI_RS,
                                        "Warning: Block height {} is less than current height {}",
                                        block_height,
                                        current_height
                                    );
                                }
                                current_height = block_height;

                                if let Err(e) = streamer_message_sink.send(streamer_msg).await {
                                    tracing::error!(target: BLOCKSAPI_RS, "Error sending message to sink: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                    Some(Err(status)) => {
                        tracing::error!(target: BLOCKSAPI_RS, "Stream error: {}", status);
                        anyhow::bail!("Stream error: {}", status);
                    }
                    None => {
                        tracing::warn!(target: BLOCKSAPI_RS, "End of stream reached");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Creates `mpsc::channel` and returns the `receiver` to read the stream of `StreamerMessage`
/// ```
/// use blocksapi_rs::BlocksApiConfigBuilder;
/// use tokio::sync::mpsc;
///
/// async fn main() {
///    let config = BlocksApiConfigBuilder::default()
///        .server_addr("http://localhost:4300".into())
///        .start_on(132545138)
///        .blocksapi_token("your_token_here".into())
///        .build()
///        .expect("Failed to build BlocksApiConfig");
///
///     let (_, stream) = blocksapi_rs::streamer(config);
///
///     while let Some(streamer_message) = stream.recv().await {
///         eprintln!("{:#?}", streamer_message);
///     }
/// }
/// ```
pub fn streamer(
    config: BlocksApiConfig,
) -> (
    tokio::task::JoinHandle<Result<(), anyhow::Error>>,
    tokio::sync::mpsc::Receiver<near_indexer_primitives::StreamerMessage>,
) {
    let (sender, receiver) = tokio::sync::mpsc::channel(config.concurrency);
    (tokio::spawn(start(sender, config)), receiver)
}
