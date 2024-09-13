use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{debug, error, info, trace};

use crate::broadcast::BroadcastManager;

pub async fn handle_socket(
    socket: WebSocket,
    broadcast_manager: Arc<BroadcastManager>,
    room_name: String,
) {
    let client_id = ulid::Ulid::new();
    info!("Client {} connected to room '{}'", client_id, room_name);

    // Split the WebSocket into sender and receiver
    let (ws_tx, ws_rx) = socket.split();

    // Prepare the sink to send binary messages
    let sink = ws_tx
        .with(|data: Vec<u8>| futures::future::ok::<Message, axum::Error>(Message::Binary(data)));
    let sink = Arc::new(Mutex::new(sink));

    // Process incoming messages
    let stream = ws_rx
        .map(move |result| {
            result
                .map_err(WebSocketError::ReceiveError)
                .and_then(|msg| match msg {
                    Message::Binary(data) => {
                        debug!("Client {} sent binary data: {:?}", client_id, data);
                        Ok(data)
                    }
                    Message::Text(text) => {
                        trace!("Client {} sent text data: {}", client_id, text);
                        Ok(text.into_bytes())
                    }
                    _ => {
                        // Unsupported message type, return error
                        Err(WebSocketError::UnsupportedMessageType)
                    }
                })
        })
        .take_while(|res| futures::future::ready(res.is_ok())); // Stop on first error

    // Subscribe the client to the room
    let subscription = match broadcast_manager
        .subscribe(&room_name, sink.clone(), stream)
        .await
    {
        Ok(sub) => sub,
        Err(e) => {
            error!("Client {} subscription error: {}", client_id, e);
            // Close the WebSocket connection
            if let Err(e) = sink.lock().await.close().await {
                error!("Failed to close WebSocket: {}", e);
            }
            return;
        }
    };

    // Wait for the subscription to complete or be cancelled
    if let Err(e) = subscription.completed().await {
        error!("Client {} subscription error: {}", client_id, e);
    }

    // // Close the WebSocket connection
    if let Err(e) = sink.lock().await.close().await {
        error!("Failed to close WebSocket: {}", e);
    }

    // Unsubscribe the client when the connection is closed
    if let Err(e) = broadcast_manager.unsubscribe(&room_name).await {
        error!("Client {} unsubscription error: {}", client_id, e);
    }

    info!(
        "Client {} disconnected from room '{}'",
        client_id, room_name
    );
}

#[derive(Debug, Error)]
enum WebSocketError {
    #[error("WebSocket receive error: {0}")]
    ReceiveError(#[from] axum::Error),
    #[error("Unsupported message type")]
    UnsupportedMessageType,
}

unsafe impl Send for WebSocketError {}
unsafe impl Sync for WebSocketError {}
