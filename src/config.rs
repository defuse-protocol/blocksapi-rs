use crate::blocksapi;

/// Configuration struct for Blocks API client
/// NB! Consider using [`BlocksApiConfigBuilder`]
/// Building the `BlocksApiConfig` example:
/// ```
/// use blocksapi_rs::BlocksApiConfigBuilder;
///
/// async fn main() {
///    let config = BlocksApiConfigBuilder::default()
///        .server_addr("http://localhost:4300".into())
///        .start_on(132545138)
///        .blocksapi_token("your_token_here".into())
///        .build()
///        .expect("Failed to build BlocksApiConfig");
/// }
/// ```
#[derive(Default, Builder)]
#[builder(pattern = "owned")]
pub struct BlocksApiConfig {
    /// Connection parameters
    ///
    /// The address of the Blocks API server.
    #[builder(setter(into))]
    pub server_addr: String,
    /// The block height to start streaming from if `start_on_latest` is false.
    #[builder(default)]
    pub start_on: Option<u64>,
    /// The name of the stream to connect to.
    /// This should match the stream name configured on the Blocks API server.
    #[builder(default = "v2_mainnet_near_blocks".to_string())]
    pub stream_name: String,
    /// Optional authentication token for the Blocks API server.
    /// If the server requires authentication, provide the token here.
    #[builder(default)]
    pub blocksapi_token: Option<String>,
    /// Streaming parameters
    ///
    /// The number of blocks to fetch in each batch.
    #[builder(default = 20)]
    pub batch_size: usize,
    /// The number of concurrent streams to open.
    #[builder(default = 100)]
    pub concurrency: usize,
    /// Endpoint configuration parameters
    ///
    /// The following parameters configure the gRPC endpoint.
    #[builder(default = 30)]
    pub tcp_keepalive: u64,
    /// The HTTP/2 keepalive interval in seconds.
    #[builder(default = 30)]
    pub http2_keepalive_interval: u64,
    /// The keepalive timeout in seconds.
    #[builder(default = 5)]
    pub keepalive_timeout: u64,
    /// The connection timeout in seconds.
    #[builder(default = 10)]
    pub connect_timeout: u64,
    /// Whether to enable HTTP/2 adaptive flow control.
    #[builder(default = true)]
    pub http2_adaptive_window: bool,
    /// The initial connection window size for gRPC.
    /// Default is 128MB.
    #[builder(default = 128 * 1024 * 1024)]
    pub connection_window_size: u32,
    /// The initial stream window size for gRPC.
    /// Default is 128MB.
    #[builder(default = 128 * 1024 * 1024)]
    pub stream_window_size: u32,
    /// The size of the buffer for incoming messages.
    /// Default is 1GB.
    #[builder(default = 1 * 1024 * 1024 * 1024)]
    pub buffer_size: usize,
    /// The maximum number of concurrent requests to the Blocks API server.
    /// Default is 1024.
    #[builder(default = 1024)]
    pub concurrency_limit: usize,
}

impl BlocksApiConfig {
    pub async fn client(
        &self,
    ) -> anyhow::Result<
        blocksapi::blocks_provider_client::BlocksProviderClient<tonic::transport::Channel>,
    > {
        let endpoint = tonic::transport::Endpoint::from_shared(self.server_addr.clone())?
            .tcp_keepalive(Some(std::time::Duration::from_secs(self.tcp_keepalive)))
            .http2_keep_alive_interval(std::time::Duration::from_secs(
                self.http2_keepalive_interval,
            ))
            .keep_alive_timeout(std::time::Duration::from_secs(self.keepalive_timeout))
            .connect_timeout(std::time::Duration::from_secs(self.connect_timeout))
            .http2_adaptive_window(self.http2_adaptive_window)
            .initial_connection_window_size(Some(self.connection_window_size))
            .initial_stream_window_size(Some(self.stream_window_size))
            .concurrency_limit(self.concurrency_limit)
            .buffer_size(self.buffer_size);

        let channel = endpoint.connect().await?;

        Ok(blocksapi::blocks_provider_client::BlocksProviderClient::new(channel))
    }

    pub async fn request(&self) -> blocksapi::ReceiveBlocksRequest {
        let mut start_policy =
            blocksapi::receive_blocks_request::StartPolicy::StartOnLatestAvailable;
        let mut start_target = None;
        if self.start_on.is_some() {
            start_policy = blocksapi::receive_blocks_request::StartPolicy::StartOnClosestToTarget;
            start_target = Some(blocksapi::block_message::Id {
                kind: blocksapi::block_message::Kind::MsgWhole as i32,
                height: self.start_on.unwrap(),
                shard_id: 0, // Default shard ID
            });
        }

        blocksapi::ReceiveBlocksRequest {
            stream_name: self.stream_name.clone(),
            stream_origin: String::new(), // Empty string for default origin
            start_policy: start_policy as i32,
            stop_policy: blocksapi::receive_blocks_request::StopPolicy::StopNever as i32,
            start_target: start_target,
            stop_target: None,
            delivery_settings: None,
            catchup_policy: blocksapi::receive_blocks_request::CatchupPolicy::CatchupStream as i32,
            catchup_delivery_settings: None,
            cached_zstd_dicts_sha3_hashes: Vec::new(), // No cached dictionaries
        }
    }

    pub async fn metadata(&self) -> tonic::metadata::MetadataMap {
        let mut metadata = tonic::metadata::MetadataMap::new();
        if let Some(token) = self.blocksapi_token.clone() {
            if !token.is_empty() {
                let auth_header = format!("Bearer {}", token);
                if let Ok(auth_value) = tonic::metadata::MetadataValue::try_from(auth_header) {
                    metadata.insert("authorization", auth_value);
                }
            }
        }
        metadata
    }
}
