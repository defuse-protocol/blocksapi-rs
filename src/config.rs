/// Configuration struct for Blocks API client
/// NB! Consider using [`BlocksApiConfigBuilder`]
/// Building the `BlocksApiConfig` example:
/// ```
/// use blocksapi::BlocksApiConfigBuilder;
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
#[derive(Clone, Default, Builder)]
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
    pub async fn client(&self) -> anyhow::Result<crate::client::BlocksApiClient> {
        crate::client::BlocksApiClient::from_config(self).await
    }
}
