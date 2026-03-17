# Feishu Nanobot

A Rust-based nanobot for Feishu (Lark) that receives messages via WebSocket and always replies with "你好".

## Prerequisites

- Rust 1.75 or later
- A Feishu (Lark) app with nanobot capabilities

## Setup

1. Create a Feishu app in the [Feishu Open Platform](https://open.feishu.cn/):
   - Go to "Manage Applications" -> "Create Application"
   - Select "Custom Application"
   - Enable "Message" capability
   - Configure the event subscription (WebSocket mode)

2. Get your app credentials from the Feishu Open Platform:
   - App ID
   - App Secret
   - Encrypt Key
   - Verification Token

3. Set up environment variables:

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env and replace with your actual credentials
nano .env
```

Or set them directly:

```bash
export FEISHU_APP_ID="your_app_id"
export FEISHU_APP_SECRET="your_app_secret"
export FEISHU_ENCRYPT_KEY="your_encrypt_key"
export FEISHU_VERIFICATION_TOKEN="your_verification_token"
```

## Running

### Development

```bash
cargo run
```

### Production

```bash
cargo build --release
./target/release/feishu-nanobot
```

The bot will connect to Feishu via WebSocket and start receiving events.

## Configuration

No web server is needed! The bot uses WebSocket to connect to Feishu's event stream:
- Events to subscribe: `im.message.receive_v1`
- The bot will automatically reconnect if the connection drops

## Architecture

The bot uses the following architecture:

1. **WebSocket Client**: Uses `feishu-sdk`'s `StreamClient` to connect to Feishu via WebSocket
2. **Event Dispatcher**: Uses `feishu-sdk`'s `EventDispatcher` to manage and route events
3. **Event Handler**: Custom `MessageEventHandler` processes incoming message events
4. **Client**: Uses `feishu-sdk`'s `Client` to interact with Feishu APIs

## Key Components

### WebSocket Stream Client

The bot uses `StreamClient` to:
- Connect to Feishu's WebSocket endpoint
- Automatically handle reconnection (with configurable interval and retry count)
- Send periodic ping/pong heartbeats
- Receive real-time event notifications

### Event Handler

The `MessageEventHandler` implements the `EventHandler` trait and:
- Listens for `im.message.receive_v1` events
- Extracts message content from incoming events
- Always replies with "你好"

### Stream Configuration

The WebSocket stream is configured with:
- Locale: "zh"
- Auto-reconnect: enabled
- Reconnect interval: 5 seconds
- Ping interval: 30 seconds

## Testing

Send any message to your bot in Feishu, and it will reply with "你好".

**Note**: The current implementation handles event reception via WebSocket. The message sending logic is a placeholder and needs to use the actual Feishu API. You'll need to:

1. Implement the actual message sending API call using `client.operation()`
2. Use the correct API endpoint based on feishu-sdk documentation
3. Handle any authentication and rate limiting

## Project Structure

```
feishu-examples/
├── src/
│   └── main.rs          # Main bot implementation
├── Cargo.toml           # Project dependencies
├── .env.example         # Environment variables template
├── .gitignore           # Git ignore rules
└── README.md            # This file
```

## Dependencies

- `feishu-sdk`: Feishu SDK for Rust (with `websocket` feature)
- `tokio`: Async runtime
- `serde`: Serialization framework
- `tracing`: Structured logging
- `anyhow`: Error handling

## Error Handling

This project uses `anyhow` for error handling with context information, following the Rust best practices for binary applications [[memory:3f9195c4-f5f7-476b-9b29-cf577907cf19]].

## Logging

The bot uses `tracing` for structured logging [[memory:7889c599-34b2-40c6-9efb-eeb2a80b976b]]. You can control log levels via the `RUST_LOG` environment variable:

```bash
RUST_LOG=debug cargo run
```

## Advantages of WebSocket over HTTP

1. **Real-time**: Events are pushed immediately without polling
2. **Efficient**: No need to maintain an HTTP server
3. **Auto-reconnect**: Built-in reconnection logic
4. **Bi-directional**: Can send and receive messages over the same connection
5. **Lower latency**: Direct connection reduces network overhead

## Known Limitations

1. The message sending logic is a placeholder and needs to use the actual Feishu API
2. The bot needs proper error handling for event decryption and validation
3. Signature verification is enabled but needs proper configuration

## Future Improvements

1. Implement actual message sending using Feishu API
2. Add support for different message types
3. Add proper error handling and recovery mechanisms
4. Add metrics and monitoring
5. Add health check status
6. Implement graceful shutdown with cleanup
