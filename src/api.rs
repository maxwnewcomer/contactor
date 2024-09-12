use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

use crate::broadcast::BroadcastManager;

#[derive(Debug, Error)]
enum WebSocketError {
    #[error("WebSocket receive error: {0}")]
    ReceiveError(#[from] axum::Error),
    #[error("Unsupported message type")]
    UnsupportedMessageType,
}

unsafe impl Send for WebSocketError {}
unsafe impl Sync for WebSocketError {}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(broadcast_manager): State<Arc<BroadcastManager>>,
    Path(room_name): Path<String>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, broadcast_manager.clone(), room_name))
}

async fn handle_socket(
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
    let stream = ws_rx.map(move |result| {
        result
            .map_err(WebSocketError::ReceiveError)
            .and_then(|msg| match msg {
                Message::Binary(data) => {
                    debug!("Client {} sent binary data: {:?}", client_id, data);
                    Ok(data)
                }
                Message::Text(text) => {
                    debug!("Client {} sent text data: {}", client_id, text);
                    Ok(text.into_bytes())
                }
                _ => Err(WebSocketError::UnsupportedMessageType),
            })
    });

    // Subscribe the client to the room
    if let Err(e) = broadcast_manager
        .subscribe(&room_name, sink.clone(), stream)
        .await
    {
        error!("Client {} subscription error: {}", client_id, e);
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
