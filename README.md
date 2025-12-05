# blocksapi-rs

Rust client utilities for streaming NEAR blockchain blocks via an Aurora Borealis Blocks API (gRPC) endpoint. It builds strongly-typed clients from the upstream `borealis-prototypes` protobuf definitions and converts streamed block messages into `near-indexer-primitives::StreamerMessage` structures ready for ingestion / indexing pipelines.

---

## Features

- gRPC streaming client (Tonic) for Borealis Blocks API `ReceiveBlocks`
- Automatic protobuf generation from the `borealis-prototypes` submodule
- Batch processing with ordered delivery guarantees
- Parallel decoding + deterministic (height-ordered) reassembly
- LZ4 + Borealis envelope decoding of NEAR block payloads (Near Block V2)
- Simple builder-based configuration (`BlocksApiConfigBuilder`)
- Backpressure via Tokio `mpsc` channel with configurable capacity
- Optional bearer token authentication

## Status

Experimental / early stage. Expect breaking API changes prior to `1.0.0`.

---

## Quick Start

```bash
git clone https://github.com/defuse-protocol/blocksapi-rs.git
cd blocksapi-rs
git submodule update --init --recursive
```
---

## Installation (as a dependency)

The crate is not (yet) published. Add it via a git dependency:

```toml
[dependencies]
blocksapi-rs = { git = "https://github.com/defuse-protocol/blocksapi-rs.git", tag = "v0.2.0" }
```

Or using a local path:

```toml
[dependencies]
blocksapi-rs = { path = "../blocksapi-rs" }
```

### Rust Version
Pinned via `rust-toolchain` to `1.87.0`.

---

## Protobuf Generation

The `build.rs` searches recursively under `proto/borealis-prototypes` for all `.proto` files and generates modules into `$OUT_DIR` using `tonic-prost-build`.

If you see:

```
Error: borealis-prototypes submodule not found!
```

Run:

```bash
git submodule update --init --recursive
```

Regenerate after upstream proto updates:

```bash
git -C proto/borealis-prototypes pull origin main
cargo clean && cargo build
```

---

## Configuration

All configuration uses `BlocksApiConfig` (build with `BlocksApiConfigBuilder`). Key fields:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `server_addr` | `String` | (required) | gRPC endpoint base URL (e.g. `http://localhost:4300`). |
| `start_on` | `Option<u64>` | `None` | Specific block height to target (closest). If `None`, starts from latest available. |
| `stream_name` | `String` | `v2_mainnet_near_blocks` | Server stream identifier. |
| `blocksapi_token` | `Option<String>` | `None` | Bearer auth token if server requires authentication. |
| `batch_size` | `usize` | `20` | Messages accumulated before parallel decode & ordered dispatch. |
| `concurrency` | `usize` | `100` | Size of internal `mpsc` channel (and spawn concurrency). |
| `tcp_keepalive` | `u64` | `30` | TCP keepalive (s). |
| `http2_keepalive_interval` | `u64` | `30` | HTTP/2 PING interval (s). |
| `keepalive_timeout` | `u64` | `5` | Keepalive timeout (s). |
| `connect_timeout` | `u64` | `10` | Connection timeout (s). |
| `http2_adaptive_window` | `bool` | `true` | Enable adaptive flow control in h2. |
| `connection_window_size` | `u32` | `134217728` | Initial connection window (bytes). |
| `stream_window_size` | `u32` | `134217728` | Initial per-stream window (bytes). |
| `buffer_size` | `usize` | `1073741824` | Tonic buffer size (bytes). |
| `concurrency_limit` | `usize` | `1024` | Max concurrent HTTP/2 streams/requests. |

Environment variables (convention used in `example.main.rs`):

| Variable | Meaning |
|----------|---------|
| `BLOCKS_API_SERVER_ADDR` | Overrides `server_addr` |
| `BLOCKS_API_START_BLOCK` | Parsed as `u64` for `start_on` |
| `BLOCKSAPI_TOKEN` | Bearer token (without `Bearer ` prefix) |

---

## Usage Pattern

1. Build a config
2. Call `streamer(config)` → returns `(JoinHandle, Receiver<StreamerMessage>)`
3. Consume messages until channel closes / end of stream
4. Await the join handle to surface terminal errors

Example:

```rust
let config = BlocksApiConfigBuilder::default()
	.server_addr("https://blocks.your-network.example")
	.start_on(123_456_789u64) // or omit
	.blocksapi_token(Some("my-secret-token".into()))
	.batch_size(50)
	.build()?;

let (task, mut rx) = blocksapi::streamer(config);
while let Some(msg) = rx.recv().await {
	// Process StreamerMessage
	do_something(&msg)?;
}
task.await??; // Propagate errors
```

---

## Decoding Pipeline (Internal)

For each `ReceiveBlocksResponse` batch:

1. Raw gRPC response → extract `BlockMessage` with `RawPayload`
2. Validate format == `PayloadNearBlockV2`
3. CBOR decode Borealis envelope (versioned wrapper) via `ciborium`
4. Extract inner LZ4-compressed block JSON blob → decompress
5. Deserialize into `near_indexer_primitives::StreamerMessage` via `serde_json`
6. Sort batch by `block.header.height` to ensure monotonic order
7. Forward downstream via `mpsc::Sender`

This ensures deterministic ordering even with parallel per-message decoding.

---

## Performance Tuning

- Increase `batch_size` to improve parallel decode efficiency (higher latency tolerance)
- Adjust `concurrency` (channel size) to accommodate downstream backpressure
- Tune HTTP/2 window sizes for high-throughput / high-latency networks
- Ensure LZ4 decompression keeps pace (consider profiling if CPU-bound)

---

## Error Handling & Logging

- Per-message decode errors are logged (`tracing::error!`) and skipped
- Stream errors bubble up → `start()` returns `Err` and join handle resolves with error
- Unexpected block height regressions emit a `tracing::warn!`

Integrate with your preferred subscriber (e.g. `tracing_subscriber::fmt()`).

---

## Development

Rebuild protos after updating submodule:

```bash
git submodule update --remote --recursive
cargo clean && cargo build
```
