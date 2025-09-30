use std::io::Read;

/// Borealis envelope structure (tuple struct, serialized as CBOR array)
#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
pub struct BorealisEnvelope(
    u16,      // type_field
    u64,      // sequential_id
    u32,      // timestamp_s
    u16,      // timestamp_ms
    [u8; 16], // unique_id
);

/// Decode the Borealis payload which is prefixed with a 1-byte version
/// Currently only version 1 is supported
/// The payload is expected to be in the format:
/// [1-byte version][BorealisEnvelope][actual payload bytes]
pub async fn decode_borealis_payload<T>(data: &[u8]) -> anyhow::Result<T>
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

/// Decode the Borealis payload containing LZ4 compressed JSON data
/// The payload is expected to be in the format:
/// [1-byte version][BorealisEnvelope][LZ4 compressed JSON bytes]
/// Returns the decompressed JSON bytes
pub async fn decode_near_block_json(data: &[u8]) -> anyhow::Result<Vec<u8>> {
    // First decode the borealis payload to get the LZ4 compressed data
    let lz4_data: Vec<u8> = decode_borealis_payload(data).await?;
    let mut buf = vec![];
    lz4::Decoder::new(std::io::Cursor::new(lz4_data))?.read_to_end(&mut buf)?;
    Ok(buf)
}

/// Patch the JSON to add missing fields for compatibility with near-indexer-primitives
/// Specifically, add "chunks": [] if missing in the block object
/// This is needed because near-indexer-primitives expects the "chunks" field to be present
/// even if it's empty
pub async fn patch_streamer_message_json(json_data: Vec<u8>) -> anyhow::Result<serde_json::Value> {
    let mut json_msg = serde_json::from_slice::<serde_json::Value>(&json_data)?;
    if let Some(block) = json_msg.get_mut("block") {
        if let Some(block_obj) = block.as_object_mut() {
            // Add chunks field as empty array if it doesn't exist
            block_obj
                .entry("chunks")
                .or_insert_with(|| serde_json::Value::Array(vec![]));
        }
    }

    Ok(json_msg)
}

/// Convert a `blocksapi::ReceiveBlocksResponse` to `near_indexer_primitives::StreamerMessage`
/// by decoding the payload and deserializing the JSON
/// This function handles only the PAYLOAD_NEAR_BLOCK_V2 format for now
pub async fn convert_to_streamer_message(
    message: crate::borealis_blocksapi::ReceiveBlocksResponse,
) -> anyhow::Result<near_indexer_primitives::StreamerMessage> {
    let msg = match message.response {
        Some(crate::borealis_blocksapi::receive_blocks_response::Response::Message(msg)) => {
            match msg.message {
                Some(block_msg) => block_msg,
                None => {
                    anyhow::bail!("Received BlockMessage with no message field");
                }
            }
        }
        _ => {
            anyhow::bail!("Received message with no response field");
        }
    };

    // Extract the raw payload
    let raw_payload =
        if let Some(crate::borealis_blocksapi::block_message::Payload::RawPayload(data)) =
            msg.payload
        {
            data
        } else {
            anyhow::bail!("Unsupported payload type or missing payload");
        };

    // Check the format - we expect PAYLOAD_NEAR_BLOCK_V2 (JSON format)
    match crate::borealis_blocksapi::block_message::Format::try_from(msg.format)? {
        crate::borealis_blocksapi::block_message::Format::PayloadNearBlockV2 => {
            // For V2, we need to decode the borealis envelope and decompress LZ4
            let json_data = decode_near_block_json(&raw_payload).await?;
            let streamer_msg_json = patch_streamer_message_json(json_data).await?;
            Ok(serde_json::from_value::<
                near_indexer_primitives::StreamerMessage,
            >(streamer_msg_json)?)
        }
        _ => {
            anyhow::bail!("Unsupported block message format: {:?}", msg.format);
        }
    }
}
