use futures::StreamExt;
use std::env;

#[derive(Default)]
pub struct SpeedTracker {
    pub last_log_time: Option<std::time::Instant>,
    pub log_interval: std::time::Duration,
    pub msgs_since_last_log: u64,
}

impl SpeedTracker {
    pub fn new() -> Self {
        Self {
            last_log_time: None,
            log_interval: std::time::Duration::from_secs(10),
            msgs_since_last_log: 0,
        }
    }

    pub fn update(&mut self, current_block: u64) {
        let now = std::time::Instant::now();

        if self.last_log_time.is_none() {
            self.last_log_time = Some(now);
        }

        self.msgs_since_last_log += 1;

        if let Some(last_time) = self.last_log_time {
            let time_diff = now.duration_since(last_time);
            if time_diff >= self.log_interval {
                let msgs_speed = self.msgs_since_last_log as f64 / time_diff.as_secs_f64();

                println!(
                    "SPEED: {:.2} BPS, CURRENT BLOCK: {}",
                    msgs_speed, current_block
                );

                self.last_log_time = Some(now);
                self.msgs_since_last_log = 0;
            }
        }
    }
}

async fn handle_streamer_message(
    message: near_indexer_primitives::StreamerMessage,
    speed_tracker: std::sync::Arc<std::sync::Mutex<SpeedTracker>>,
) -> anyhow::Result<()> {
    println!("Received block: {}", message.block.header.height);

    let mut speed_tracker = speed_tracker.lock().unwrap();
    speed_tracker.update(message.block.header.height);
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Get configuration from environment variables
    let server_addr =
        env::var("BLOCKS_API_SERVER_ADDR").unwrap_or_else(|_| "http://localhost:4300".to_string());
    let start_block = env::var("BLOCKS_API_START_BLOCK")
        .ok()
        .and_then(|val| val.parse::<u64>().ok());
    let api_token = env::var("BLOCKSAPI_TOKEN").ok();

    let config = blocksapi::BlocksApiConfigBuilder::default()
        .server_addr(server_addr)
        .start_on(Some(start_block.unwrap_or(132545138)))
        .blocksapi_token(api_token)
        .build()?;

    let (sender, stream) = blocksapi::streamer(config);
    let speed_tracker = std::sync::Arc::new(std::sync::Mutex::new(SpeedTracker::new()));

    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|streamer_message| handle_streamer_message(streamer_message, speed_tracker.clone()))
        .buffer_unordered(1);

    while let Some(_handle_message) = handlers.next().await {
        if let Err(err) = _handle_message {
            println!("Error: {:?}", err);
        }
    }
    drop(handlers); // close the channel so the sender will stop

    // propagate errors from the sender
    match sender.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(anyhow::Error::from(e)), // JoinError
    }
}
