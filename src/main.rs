use anyhow::{Context, Result};
use feishu_sdk::client::Client;
use feishu_sdk::core::{Config, LogLevel};
use feishu_sdk::event::{Event, EventHandler, EventHandlerResult, EventDispatcher, EventDispatcherConfig};
use feishu_sdk::ws::stream::{StreamClientBuilder, StreamConfig};
use std::pin::Pin;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Custom logger that forwards SDK logs to tracing
#[derive(Debug)]
struct TracingLogger;

impl TracingLogger {
    fn new() -> Self {
        Self
    }
}

impl feishu_sdk::core::Logger for TracingLogger {
    fn log(&self, level: LogLevel, message: &str) {
        match level {
            LogLevel::Debug => tracing::debug!("[SDK] {}", message),
            LogLevel::Info => tracing::info!("[SDK] {}", message),
            LogLevel::Warn => tracing::warn!("[SDK] {}", message),
            LogLevel::Error => tracing::error!("[SDK] {}", message),
        }
    }

    fn is_enabled(&self, _level: LogLevel) -> bool {
        true
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing subscriber
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Load configuration from environment variables
    let app_id = std::env::var("FEISHU_APP_ID")
        .context("FEISHU_APP_ID environment variable must be set")?;
    let app_secret = std::env::var("FEISHU_APP_SECRET")
        .context("FEISHU_APP_SECRET environment variable must be set")?;
    // let encrypt_key = std::env::var("FEISHU_ENCRYPT_KEY")
    //     .context("FEISHU_ENCRYPT_KEY environment variable must be set")?;
    // let verification_token = std::env::var("FEISHU_VERIFICATION_TOKEN")
    //     .context("FEISHU_VERIFICATION_TOKEN environment variable must be set")?;

    info!("Starting feishu nanobot with app_id: {}", app_id);

    // Create Feishu client configuration
    let config = Config::builder(&app_id, &app_secret).build();

    // Create Feishu client
    let client = Client::new(config.clone())
        .context("Failed to create Feishu client")?;

    // Create event dispatcher
    let dispatcher_config = EventDispatcherConfig::new()
        .verification_token("")
        .encrypt_key("")
        .skip_signature_verification(true);

    let dispatcher = EventDispatcher::new(
        dispatcher_config,
        Arc::new(TracingLogger::new()),
    );

    // Register message event handler
    dispatcher.register_handler(Box::new(MessageEventHandler::new(client))).await;

    info!("Event handlers registered");

    // Create stream configuration for WebSocket
    let stream_config = StreamConfig::new()
        .locale("zh")
        .auto_reconnect(true)
        .reconnect_interval(tokio::time::Duration::from_secs(5))
        .ping_interval(tokio::time::Duration::from_secs(30));

    // Create StreamClient using WebSocket
    let stream_client = StreamClientBuilder::new(config)
        .stream_config(stream_config)
        .event_dispatcher(dispatcher)
        .build()
        .context("Failed to build WebSocket stream client")?;

    info!("WebSocket stream client created successfully");

    // Spawn the WebSocket connection task
    info!("Connecting to Feishu WebSocket stream...");
    let stream_handle = stream_client.spawn();

    info!("WebSocket connection task spawned successfully");
    info!("Nanobot is now listening for events via WebSocket");
    
    // Wait for Ctrl+C to gracefully shutdown
    tokio::select! {
        result = stream_handle => {
            match result {
                Ok(Ok(())) => {
                    info!("WebSocket connection closed gracefully");
                    Ok(())
                }
                Ok(Err(e)) => {
                    error!("WebSocket connection error: {:?}", e);
                    Err(e).context("WebSocket connection failed")
                }
                Err(e) => {
                    error!("Task join error: {:?}", e);
                    Err(e).context("Task join failed")
                }
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal");
            Ok(())
        }
    }
}

struct MessageEventHandler {
    client: Client,
}

impl MessageEventHandler {
    fn new(client: Client) -> Self {
        Self { client }
    }

    async fn send_reply(&self, chat_id: &str, message: &str) -> Result<()> {
        info!("Sending reply to chat {}: {}", chat_id, message);

        use feishu_sdk::api::{SendMessageBody, SendMessageQuery};
        use feishu_sdk::core::RequestOptions;

        // Build content JSON string
        let content_text = message.replace('"', "\\\"");
        let content = format!(r#"{{"text":"{}"}}"#, content_text);

        let body = SendMessageBody {
            receive_id: chat_id.to_string(),
            msg_type: "text".to_string(),
            content,
            uuid: None,
        };

        let query = SendMessageQuery {
            receive_id_type: Some("chat_id".to_string()),
        };

        info!("Request - receive_id: {}, msg_type: text", chat_id);

        let response = self
            .client
            .im_v1_message()
            .send_typed(&query, &body, RequestOptions::default())
            .await
            .context("Failed to send message to Feishu API")?;

        if response.code == 0 {
            info!(
                "Message sent successfully to chat {}",
                chat_id
            );
            if let Some(msg_id) = response.data.and_then(|d| d.message_id) {
                info!("Message ID: {}", msg_id);
            }
        } else {
            error!(
                "Failed to send message. code={}, msg={}",
                response.code, response.msg
            );
        }

        Ok(())
    }
}

impl EventHandler for MessageEventHandler {
    fn event_type(&self) -> &str {
        "im.message.receive_v1"
    }

    fn handle(&self, event: Event) -> Pin<Box<dyn std::future::Future<Output = EventHandlerResult> + Send + '_>> {
        Box::pin(async move {
            info!("Received message event: {:?}", event.event_type());

            // Log full event JSON for debugging
            if let Ok(event_json) = serde_json::to_string_pretty(&event) {
                debug!("Full event JSON: {}", event_json);
            }

            // Extract raw event data (Option<serde_json::Value>)
            if let Some(event_data) = &event.event {
                // Try to parse event data into structured MessageEvent
                match serde_json::from_value::<feishu_sdk::event::models::im::MessageEvent>(event_data.clone()) {
                    Ok(msg_event) => {
                        info!("Parsed message event successfully");

                        if let Some(chat_id) = msg_event.message.chat_id.as_deref() {
                            info!("Message from chat: {}", chat_id);

                            // Check if message has mentions (AT)
                            if let Some(mentions) = &msg_event.message.mentions {
                                info!("Message has {} mentions", mentions.len());
                                for mention in mentions {
                                    info!("Mention: {:?}", mention);
                                }
                            }

                            // Get message content
                            let message_type = msg_event.message.message_type.as_deref().unwrap_or("unknown");
                            info!("Message type: {}", message_type);

                            if let Some(content) = &msg_event.message.content {
                                info!("Raw message content: {}", content);

                                // Parse content JSON based on message type
                                if message_type == "text" {
                                    if let Ok(text_content) = serde_json::from_str::<serde_json::Value>(content) {
                                        if let Some(text) = text_content.get("text").and_then(|v| v.as_str()) {
                                            info!("Parsed text content: {}", text);
                                        }
                                    }
                                } else {
                                    info!("Received non-text message type: {}", message_type);
                                }
                            }

                            // Always reply with "你好"
                            info!("Attempting to send reply to chat {}", chat_id);
                            match self.send_reply(chat_id, "你好").await {
                                Ok(_) => {
                                    info!("Reply sent successfully to chat {}", chat_id);
                                }
                                Err(e) => {
                                    error!("Failed to send reply: {:?}", e);
                                }
                            }
                        } else {
                            warn!("Message event missing chat_id");
                        }
                    }
                    Err(e) => {
                        error!("Failed to parse message event: {:?}", e);

                        // Fallback: try to extract data from raw event
                        info!("Attempting fallback parsing from raw event");
                        if let Some(message) = event_data.get("message") {
                            if let Some(chat_id) = message.get("chat_id").and_then(|v| v.as_str()) {
                                info!("Fallback: Found chat_id in raw event: {}", chat_id);

                                // Try to send reply anyway
                                if let Err(e) = self.send_reply(chat_id, "你好 (fallback)").await {
                                    error!("Fallback reply failed: {:?}", e);
                                }
                            } else {
                                error!("Fallback: No chat_id found in message");
                            }
                        } else {
                            error!("Fallback: No message field found in event");
                        }
                    }
                }
            } else {
                error!("No event data available");
            }

            Ok(None)
        })
    }
}

