use axum::{
    extract::{ws::WebSocketUpgrade, Path, State},
    response::IntoResponse,
};
use redis::AsyncCommands;
use std::sync::Arc;
use tracing::error;

use crate::{broadcast::BroadcastManager, relay::build_relay, ws::handle_socket};

pub struct AppState {
    pub broadcast_manager: Arc<BroadcastManager>,
    pub redis_client: redis::Client,
    pub node_address: String, // Unique identifier or address of this server
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    Path(room_name): Path<String>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| {
        let state = state.clone();
        let room_name = room_name.clone();
        async move {
            let room_key = format!("room:{}", room_name);

            let mut redis_conn = match state.redis_client.get_multiplexed_async_connection().await {
                Ok(conn) => conn,
                Err(err) => {
                    error!("Failed to get Redis connection: {}", err);
                    return;
                }
            };

            // Attempt to get the server address for the room
            let room_server: Option<String> = match redis_conn.get(&room_key).await {
                Ok(server) => server,
                Err(err) => {
                    error!("Failed to get room server from Redis: {}", err);
                    return;
                }
            };

            if let Some(server_address) = room_server {
                if server_address == state.node_address {
                    // Room is hosted on this server
                    handle_socket(socket, state.broadcast_manager.clone(), room_name).await;
                } else {
                    // Room is on a different server; build a relay
                    build_relay(socket, server_address, room_name, state).await;
                }
            } else {
                // Room does not exist; attempt to create it atomically
                let set_result: bool = match redis_conn.set_nx(&room_key, &state.node_address).await
                {
                    Ok(result) => result,
                    Err(err) => {
                        error!("Failed to set room server in Redis: {}", err);
                        return;
                    }
                };

                if set_result {
                    // Successfully created the room
                    handle_socket(socket, state.broadcast_manager.clone(), room_name).await;
                } else {
                    // Another server created the room; get the updated server address
                    let server_address: String = match redis_conn.get(&room_key).await {
                        Ok(server) => server,
                        Err(err) => {
                            error!("Failed to get room server from Redis after set_nx: {}", err);
                            return;
                        }
                    };
                    build_relay(socket, server_address, room_name, state).await;
                }
            }
        }
    })
}
