use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures::{FutureExt, SinkExt, StreamExt};
use redis::AsyncCommands;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message as TungsteniteMessage;
use tracing::error;

use crate::api::AppState;
use crate::ws::handle_socket;

pub async fn build_relay(
    socket: WebSocket,
    initial_server_address: String,
    room_name: String,
    state: Arc<AppState>,
) {
    let mut current_server_address = initial_server_address;
    let mut socket = Some(socket);

    loop {
        let room_server_url = format!("ws://{}/ws/{}", current_server_address, room_name);

        match connect_async(&room_server_url).await {
            Ok((room_ws_stream, _)) => {
                // Split both sockets
                let (mut client_ws_sink, mut client_ws_stream) = socket.take().unwrap().split();
                let (mut room_ws_sink, mut room_ws_stream) = room_ws_stream.split();

                // Relay messages from client to room server
                let client_to_room = async {
                    while let Some(msg) = client_ws_stream.next().await {
                        if let Ok(msg) = msg {
                            let msg = match msg {
                                Message::Text(text) => TungsteniteMessage::Text(text),
                                Message::Binary(bin) => TungsteniteMessage::Binary(bin),
                                _ => continue,
                            };
                            if room_ws_sink.send(msg).await.is_err() {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                };

                // Relay messages from room server to client
                let room_to_client = async {
                    while let Some(msg) = room_ws_stream.next().await {
                        if let Ok(msg) = msg {
                            let msg = match msg {
                                TungsteniteMessage::Text(text) => Message::Text(text),
                                TungsteniteMessage::Binary(bin) => Message::Binary(bin),
                                _ => continue,
                            };
                            if client_ws_sink.send(msg).await.is_err() {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                };

                // Run both futures concurrently
                futures::future::select(client_to_room.boxed(), room_to_client.boxed()).await;
                break; // Exit the loop after successful relay
            }
            Err(err) => {
                error!(
                    "Failed to connect to room server at {}: {}",
                    room_server_url, err
                );
                // Assume the room server is dead; attempt to take over the room
                if let Some(s) = socket.take() {
                    let result = attempt_room_takeover(
                        s,
                        current_server_address.clone(),
                        room_name.clone(),
                        state.clone(),
                    )
                    .await;
                    match result {
                        Ok(new_socket) => {
                            // If takeover was successful, handle the socket directly
                            handle_socket(new_socket, state.broadcast_manager.clone(), room_name)
                                .await;
                            break;
                        }
                        Err(Some((new_socket, new_server_address))) => {
                            // Another server took over; retry with the new server address
                            socket = Some(new_socket);
                            current_server_address = new_server_address;
                            continue; // Retry the loop with the new server address
                        }
                        Err(None) => {
                            // Failed to take over and no new server address; cannot proceed
                            break;
                        }
                    }
                } else {
                    // Socket has already been taken; cannot proceed
                    break;
                }
            }
        }
    }
}

async fn attempt_room_takeover(
    socket: WebSocket,
    dead_server_address: String,
    room_name: String,
    state: Arc<AppState>,
) -> Result<WebSocket, Option<(WebSocket, String)>> {
    let room_key = format!("room:{}", room_name);
    let mut redis_conn = match state.redis_client.get_multiplexed_async_connection().await {
        Ok(conn) => conn,
        Err(err) => {
            error!("Failed to get Redis connection: {}", err);
            return Err(None);
        }
    };

    // Lua script to atomically check and delete the key if it matches the dead server
    let script = redis::Script::new(
        r#"
        local current = redis.call('GET', KEYS[1])
        if current == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        else
            return 0
        end
        "#,
    );

    let result: i32 = match script
        .key(&room_key)
        .arg(&dead_server_address)
        .invoke_async(&mut redis_conn)
        .await
    {
        Ok(res) => res,
        Err(err) => {
            error!("Failed to execute Redis script: {}", err);
            return Err(None);
        }
    };

    if result > 0 {
        // Successfully removed the old mapping; attempt to create the room
        let set_result: bool = match redis_conn.set_nx(&room_key, &state.server_address).await {
            Ok(result) => result,
            Err(err) => {
                error!("Failed to set room server in Redis: {}", err);
                return Err(None);
            }
        };

        if set_result {
            // Successfully took over the room
            Ok(socket)
        } else {
            // Another server took over the room; get the new server address
            let new_server_address: String = match redis_conn.get(&room_key).await {
                Ok(server) => server,
                Err(err) => {
                    error!("Failed to get room server from Redis after set_nx: {}", err);
                    return Err(None);
                }
            };
            Err(Some((socket, new_server_address)))
        }
    } else {
        // Another process updated the key; get the new server address
        let new_server_address: String = match redis_conn.get(&room_key).await {
            Ok(server) => server,
            Err(err) => {
                error!(
                    "Failed to get room server from Redis after DEL failed: {}",
                    err
                );
                return Err(None);
            }
        };
        Err(Some((socket, new_server_address)))
    }
}
