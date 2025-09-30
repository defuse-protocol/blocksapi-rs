use crate::borealis_blocksapi;

type BlocksProvider =
    borealis_blocksapi::blocks_provider_client::BlocksProviderClient<tonic::transport::Channel>;

pub struct BlocksApiClient {
    provider: BlocksProvider,
    metadata: tonic::metadata::MetadataMap,
    stream_name: String,
}

impl BlocksApiClient {
    async fn prepare_provider(
        config: &crate::config::BlocksApiConfig,
    ) -> anyhow::Result<BlocksProvider> {
        let endpoint = tonic::transport::Endpoint::from_shared(config.server_addr.clone())?
            .tcp_keepalive(Some(std::time::Duration::from_secs(config.tcp_keepalive)))
            .http2_keep_alive_interval(std::time::Duration::from_secs(
                config.http2_keepalive_interval,
            ))
            .keep_alive_timeout(std::time::Duration::from_secs(config.keepalive_timeout))
            .connect_timeout(std::time::Duration::from_secs(config.connect_timeout))
            .http2_adaptive_window(config.http2_adaptive_window)
            .initial_connection_window_size(Some(config.connection_window_size))
            .initial_stream_window_size(Some(config.stream_window_size))
            .concurrency_limit(config.concurrency_limit)
            .buffer_size(config.buffer_size);

        let channel = endpoint.connect().await?;

        Ok(borealis_blocksapi::blocks_provider_client::BlocksProviderClient::new(channel))
    }

    async fn prepare_metadata(
        config: &crate::config::BlocksApiConfig,
    ) -> tonic::metadata::MetadataMap {
        let mut metadata = tonic::metadata::MetadataMap::new();
        if let Some(token) = config.blocksapi_token.clone() {
            if !token.is_empty() {
                let auth_header = format!("Bearer {}", token);
                if let Ok(auth_value) = tonic::metadata::MetadataValue::try_from(auth_header) {
                    metadata.insert("authorization", auth_value);
                }
            }
        }
        metadata
    }

    pub async fn from_config(config: &crate::config::BlocksApiConfig) -> anyhow::Result<Self> {
        let provider = Self::prepare_provider(config).await?;
        let metadata = Self::prepare_metadata(config).await;
        Ok(Self {
            provider,
            metadata,
            stream_name: config.stream_name.clone(),
        })
    }

    async fn streaming_request(
        &self,
        block_height: Option<u64>,
    ) -> tonic::Request<borealis_blocksapi::ReceiveBlocksRequest> {
        let (start_policy, start_target_block) = if let Some(block_height) = block_height {
            (
                borealis_blocksapi::receive_blocks_request::StartPolicy::StartOnClosestToTarget,
                Some(borealis_blocksapi::block_message::Id {
                    kind: borealis_blocksapi::block_message::Kind::MsgWhole as i32,
                    height: block_height,
                    shard_id: 0, // Default shard ID
                }),
            )
        } else {
            (
                borealis_blocksapi::receive_blocks_request::StartPolicy::StartOnLatestAvailable,
                None,
            )
        };

        // Create the streaming request
        let stream_request = borealis_blocksapi::ReceiveBlocksRequest {
            stream_name: self.stream_name.clone(),
            stream_origin: String::new(), // Empty string for default origin
            start_policy: start_policy as i32,
            stop_policy: borealis_blocksapi::receive_blocks_request::StopPolicy::StopNever as i32,
            start_target: start_target_block,
            stop_target: None,
            delivery_settings: None,
            catchup_policy: borealis_blocksapi::receive_blocks_request::CatchupPolicy::CatchupStream
                as i32,
            catchup_delivery_settings: None,
            cached_zstd_dicts_sha3_hashes: Vec::new(), // No cached dictionaries
        };

        // Prepare the gRPC request with metadata
        let mut request = tonic::Request::new(stream_request);
        *request.metadata_mut() = self.metadata.clone();
        request
    }

    /// Stream blocks starting from the specified block height.
    /// If `block_height` is `None`, start from the latest available block.
    pub async fn get_stream(
        &self,
        block_height: Option<u64>,
    ) -> anyhow::Result<tonic::Streaming<borealis_blocksapi::ReceiveBlocksResponse>> {
        let request = self.streaming_request(block_height).await;
        let streaming = self.provider.clone().receive_blocks(request).await?;
        Ok(streaming.into_inner())
    }
}
