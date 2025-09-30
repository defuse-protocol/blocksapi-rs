#[macro_use]
extern crate derive_builder;

pub use near_indexer_primitives;

use futures::StreamExt;
use tokio::sync::mpsc;

pub mod client;
pub use client::BlocksApiClient;
pub mod config;
pub use config::{BlocksApiConfig, BlocksApiConfigBuilder};
pub mod utils;

// Generated protobuf code from aurora-is-near/borealis-prototypes
// The proto files need to be obtained from: https://github.com/aurora-is-near/borealis-prototypes
#[allow(clippy::all)]
pub mod borealis_blocksapi {
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

pub(crate) const BLOCKSAPI: &str = "blocksapi";

const MAX_ATTEMPTS: u64 = 5;
const RETRY_DELAY_SECS: u64 = 1;

/// Task to continuously update the final block height
/// This runs in the background and updates the `final_block_height` atomic variable
/// It connects to the Blocks API and listens for new blocks, updating the height accordingly
async fn task_update_final_block_regularly(
    final_block_height: std::sync::Arc<std::sync::atomic::AtomicU64>,
    config: BlocksApiConfig,
) -> anyhow::Result<()> {
    let client = config.client().await?;
    let mut stream = client.get_stream(None).await?;
    loop {
        tokio::select! {
            // Handle incoming messages
            message = stream.next() => {
                match message {
                    Some(Ok(message)) => {
                        match message.response {
                            Some(crate::borealis_blocksapi::receive_blocks_response::Response::Message(msg)) => {
                                match msg.message {
                                    Some(block_msg) => {
                                        if let Some(id) = block_msg.id {
                                            final_block_height.store(id.height, std::sync::atomic::Ordering::SeqCst);
                                        }
                                    },
                                    None => {
                                        tracing::error!(target: BLOCKSAPI, "Received BlockMessage with no message field");
                                        anyhow::bail!("Received BlockMessage with no message field");
                                    }
                                }
                            }
                            _ => {
                                tracing::error!(target: BLOCKSAPI, "Received non-message response in tip_final_block");
                                anyhow::bail!("Received message with no response field");
                            }
                        };
                    }
                    Some(Err(status)) => {
                        tracing::error!(target: BLOCKSAPI, "Stream error: {}", status);
                        anyhow::bail!("Stream error: {}", status);
                    }
                    None => {
                        tracing::error!(target: BLOCKSAPI, "End of stream reached");
                        anyhow::bail!("End of stream reached");
                    }
                }
            }
        }
    }
}

/// Start streaming blocks from the Blocks API server based on the provided configuration.
/// Blocks are sent to the provided `streamer_message_sink` channel.
/// This function manages the streaming, including handling catch-up logic and
/// ensuring blocks are processed in order.
async fn start(
    streamer_message_sink: mpsc::Sender<near_indexer_primitives::StreamerMessage>,
    config: BlocksApiConfig,
) -> anyhow::Result<()> {
    // Spawn a task to continuously update the final block height
    let final_block_height_atomic = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    tokio::spawn(task_update_final_block_regularly(
        final_block_height_atomic.clone(),
        config.clone(),
    ));

    let mut final_block_height =
        final_block_height_atomic.load(std::sync::atomic::Ordering::SeqCst);

    // Wait for the final block height to be updated if it's still 0
    if final_block_height == 0 {
        tracing::info!(target: BLOCKSAPI, "Waiting for final block height to be available...");
        let mut retries = 0;
        while final_block_height == 0 && retries < MAX_ATTEMPTS {
            tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_DELAY_SECS)).await;
            final_block_height =
                final_block_height_atomic.load(std::sync::atomic::Ordering::SeqCst);
            retries += 1;
            if final_block_height == 0 {
                tracing::warn!(target: BLOCKSAPI, "Final block height still 0, retrying... ({}/{})", retries, MAX_ATTEMPTS);
            }
        }

        if final_block_height == 0 {
            anyhow::bail!(
                "Failed to get final block height after {} seconds",
                RETRY_DELAY_SECS * MAX_ATTEMPTS
            );
        }

        tracing::debug!(target: BLOCKSAPI, "Final block height available: {}", final_block_height);
    }

    // Adjust batch size based on catch-up status
    // If we're behind, use the configured batch size
    // If we're caught up, process one by one to minimize latency
    let start_block_height = config.start_on.unwrap_or(final_block_height);
    let mut is_catchup = final_block_height > start_block_height + config.batch_size as u64;
    let mut batch_size = if is_catchup { config.batch_size } else { 1 };

    // Process the message synchronously to maintain block order
    // Collect a batch of messages and process them in parallel
    // Collect messages in batches, process in parallel, and send in order
    let mut batch: Vec<borealis_blocksapi::ReceiveBlocksResponse> = Vec::new();
    let mut current_height: u64 = start_block_height;

    tracing::info!(target: BLOCKSAPI, "Starting stream from block {}, final_block_height: {}, initial catchup: {}, batch size: {}", start_block_height, final_block_height, is_catchup, batch_size);

    let client = config.client().await?;
    let mut stream = client.get_stream(Some(start_block_height)).await?;

    loop {
        tokio::select! {
            // Handle incoming messages
            message = stream.next() => {
                match message {
                    Some(Ok(response)) => {

                        // Add current response to batch
                        batch.push(response);

                        // Process batch when it reaches the batch size
                        if batch.len() >= batch_size {
                            let batch = std::mem::take(&mut batch);
                            let mut results = Vec::with_capacity(batch.len());

                            // Process in parallel
                            let mut handles = Vec::new();
                            for msg in batch {
                                let handle = tokio::spawn(utils::convert_to_streamer_message(msg));
                                handles.push(handle);
                            }

                            // Collect results
                            for handle in handles {
                                match handle.await {
                                    Ok(Ok(streamer_msg)) => {
                                        results.push(streamer_msg);
                                    },
                                    Ok(Err(e)) => {
                                        tracing::error!(target: BLOCKSAPI, "Error converting message: {}", e);
                                    },
                                    Err(e) => {
                                        tracing::error!(target: BLOCKSAPI, "Task error: {}", e);
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
                                        target: BLOCKSAPI,
                                        "Block height {} is less than current height {}",
                                        block_height,
                                        current_height
                                    );
                                }
                                current_height = block_height;

                                if let Err(e) = streamer_message_sink.send(streamer_msg).await {
                                    tracing::error!(target: BLOCKSAPI, "Error sending message to sink: {}", e);
                                    return Err(anyhow::anyhow!("Error sending message to sink: {}", e));
                                }
                            }
                            // Update catchup status
                            let final_height = final_block_height_atomic.load(std::sync::atomic::Ordering::SeqCst);
                            let checked_is_catchup = final_height > current_height + config.batch_size as u64;
                            if is_catchup != checked_is_catchup {
                                is_catchup = checked_is_catchup;
                                batch_size = if is_catchup { config.batch_size } else { 1 };
                                tracing::info!(target: BLOCKSAPI, "Catchup status changed to: {}, new batch size: {}", is_catchup, batch_size);
                            }
                        }
                    }
                    Some(Err(status)) => {
                        tracing::error!(target: BLOCKSAPI, "Stream error: {}", status);
                        anyhow::bail!("Stream error: {}", status);
                    }
                    None => {
                        tracing::error!(target: BLOCKSAPI, "End of stream reached");
                        anyhow::bail!("End of stream reached");
                    }
                }
            }
        }
    }
}

/// Creates `mpsc::channel` and returns the `receiver` to read the stream of `StreamerMessage`
/// ```
/// use blocksapi::BlocksApiConfigBuilder;
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
///     let (_, stream) = blocksapi::streamer(config);
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
