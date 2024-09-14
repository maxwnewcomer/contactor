use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket};
use futures::{FutureExt, SinkExt, StreamExt};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sysinfo::System;
use thiserror::Error;
use tokio::sync::{watch, Mutex, RwLock};
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message as TungsteniteMessage;
use tracing::{debug, error, info, trace};
use yrs::sync::Awareness;
use yrs::Doc;

use crate::broadcast::{BroadcastManager, BroadcastManagerError};
use crate::ids::IdFactory;
use crate::RedisKeygenerator;

pub struct RelayNode {
    pub address: String,
    pub id: String,
    redis: redis::Client,
    broadcast_manager: Arc<BroadcastManager>,
    id_factory: Arc<IdFactory>,
}

#[derive(Serialize)]
struct NodeInfo {
    address: String,
    num_rooms: usize,
    num_connections: usize,
    cpu_usage: f32,
    total_memory: u64,
    used_memory: u64,
}

#[derive(Serialize, Deserialize)]
struct RoomInfo {
    address: String,
    node_id: String,
    participants: Option<usize>,
}

impl RelayNode {
    pub fn new(address: String, redis: redis::Client) -> Self {
        let factory = IdFactory::new();
        let relay_node = RelayNode {
            address: address.clone(),
            id: factory.gen_id(),
            redis: redis.clone(),
            broadcast_manager: Arc::new(BroadcastManager::new()),
            id_factory: Arc::new(factory),
        };

        // Start the node info worker
        relay_node.start_node_info_worker();

        relay_node
    }

    fn start_node_info_worker(&self) {
        let redis_clone = self.redis.clone();
        let broadcast_manager_clone = self.broadcast_manager.clone();
        let address_clone = self.address.clone();
        let id = self.id.clone();

        // Spawn the node info worker as a background task
        tokio::spawn(async move {
            // Initialize system info collector
            let mut sys = System::new_all();

            loop {
                // Update system info
                sys.refresh_all();

                // Collect data
                let num_rooms = broadcast_manager_clone.num_rooms();
                let num_connections = broadcast_manager_clone.total_listeners();

                // Collect host information
                let cpu_usage = sys.global_cpu_usage();
                let total_memory = sys.total_memory();
                let used_memory = sys.used_memory();

                // Create the info to report
                let node_info = NodeInfo {
                    address: address_clone.clone(),
                    num_rooms,
                    num_connections,
                    cpu_usage,
                    total_memory,
                    used_memory,
                };

                // Report to Redis
                let mut redis_conn = match redis_clone.get_multiplexed_async_connection().await {
                    Ok(conn) => conn,
                    Err(err) => {
                        error!("Failed to get Redis connection: {}", err);
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                let node_info_key = RedisKeygenerator::node_key(&id);

                // Serialize node_info to JSON
                let node_info_json = match serde_json::to_string(&node_info) {
                    Ok(json) => json,
                    Err(err) => {
                        error!("Failed to serialize node info: {}", err);
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                // Store in Redis with an expiry
                if let Err(err) = redis_conn
                    .set_ex::<_, _, ()>(&node_info_key, node_info_json, 10)
                    .await
                {
                    error!("Failed to set node info in Redis: {}", err);
                }

                // Sleep before next report
                sleep(Duration::from_secs(5)).await;
            }
        });
    }

    fn start_room_info_worker(&self, room_name: String, mut shutdown_rx: watch::Receiver<()>) {
        let redis_clone = self.redis.clone();
        let id = self.id.clone();
        let address_clone = self.address.clone();
        let broadcast_clone = self.broadcast_manager.clone();
        let room_key = RedisKeygenerator::room_key(&room_name);

        // Spawn the room info worker as a background task
        tokio::spawn(async move {
            loop {
                // Check for shutdown signal
                if shutdown_rx.has_changed().unwrap_or(false) {
                    info!("Shutting down room info worker for room '{}'", room_name);
                    break;
                }

                // Create the room info to report
                let room_info = RoomInfo {
                    address: address_clone.clone(),
                    node_id: id.clone(),
                    participants: broadcast_clone.listeners(&room_name),
                };

                // Report to Redis
                let mut redis_conn = match redis_clone.get_multiplexed_async_connection().await {
                    Ok(conn) => conn,
                    Err(err) => {
                        error!("Failed to get Redis connection: {}", err);
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                // Serialize room_info to JSON
                let room_info_json = match serde_json::to_string(&room_info) {
                    Ok(json) => json,
                    Err(err) => {
                        error!("Failed to serialize room info: {}", err);
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                // Store in Redis with an expiry (TTL)
                if let Err(err) = redis_conn
                    .set_ex::<_, _, ()>(&room_key, room_info_json, 10)
                    .await
                {
                    error!("Failed to set room info in Redis: {}", err);
                }

                // Sleep before next report
                tokio::select! {
                    _ = sleep(Duration::from_secs(5)) => {},
                    _ = shutdown_rx.changed() => {
                        info!("Shutting down room info worker for room '{}'", room_name);
                        break;
                    }
                }
            }
        });
    }

    pub async fn handle_upgrade(&self, socket: WebSocket, room_name: String) {
        let room_key = RedisKeygenerator::room_key(&room_name);
        let mut redis_conn = match self.redis.get_multiplexed_async_connection().await {
            Ok(conn) => conn,
            Err(err) => {
                error!("Failed to get Redis connection: {}", err);
                return;
            }
        };

        // Attempt to get the room info from Redis
        let room_info_json: Option<String> = match redis_conn.get(&room_key).await {
            Ok(info) => info,
            Err(err) => {
                error!("Failed to get room info from Redis: {}", err);
                return;
            }
        };

        if let Some(room_info_json) = room_info_json {
            // Parse the room info
            let room_info: RoomInfo = match serde_json::from_str(&room_info_json) {
                Ok(info) => info,
                Err(err) => {
                    error!("Failed to parse room info JSON: {}", err);
                    return;
                }
            };

            if room_info.address == self.address {
                // Room is hosted on this server
                debug!("Handling connection to '{}' locally", room_name);
                self.handle_socket(socket, room_name.clone()).await;
            } else {
                debug!(
                    "Expecting room '{}' to be hosted at {}, building relay",
                    room_name, room_info.address
                );
                // Room is on a different server; build a relay
                self.build_relay(socket, room_info.address.clone(), room_name.clone())
                    .await;
            }
        } else {
            // Room does not exist or TTL expired; attempt to create it atomically

            // Create room locally so that once new connections get sent the room is ready
            match self
                .broadcast_manager
                .create_room(
                    &room_name,
                    Arc::new(RwLock::new(Awareness::new(Doc::new()))),
                )
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    error!(
                        "Failed to create room '{}' in broadcast manager: {}",
                        room_name,
                        e.to_string()
                    );
                    return;
                }
            }

            // Try to set the room info in Redis with NX and TTL
            let room_info = RoomInfo {
                address: self.address.clone(),
                node_id: self.id.clone(),
                participants: self.broadcast_manager.listeners(&room_name),
            };

            // Serialize room_info to JSON
            let room_info_json = match serde_json::to_string(&room_info) {
                Ok(json) => json,
                Err(err) => {
                    error!("Failed to serialize room info: {}", err);
                    return;
                }
            };

            // Use a Lua script to set the room info with NX and TTL atomically
            let script = redis::Script::new(
                r#"
                local setnx = redis.call('SETNX', KEYS[1], ARGV[1])
                if setnx == 1 then
                    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
                end
                return setnx
                "#,
            );

            let ttl_seconds = 10;
            let set_result: i32 = match script
                .key(&room_key)
                .arg(&room_info_json)
                .arg(ttl_seconds)
                .invoke_async(&mut redis_conn)
                .await
            {
                Ok(result) => result,
                Err(err) => {
                    error!("Failed to set room info in Redis: {}", err);
                    return;
                }
            };

            if set_result == 1 {
                // Successfully created the room in Redis
                debug!("Room '{}' didn't exist so I created it", room_name);

                // Start the room info worker to refresh TTL
                let (shutdown_tx, shutdown_rx) = watch::channel(());
                self.broadcast_manager
                    .store_room_shutdown_signal(&room_name, shutdown_tx)
                    .await;

                self.start_room_info_worker(room_name.clone(), shutdown_rx);
                // Handle the socket
                self.handle_socket(socket, room_name.clone()).await;
            } else {
                // Another server created the room; get the updated room info

                // Tear down local room
                if let Err(e) = self.broadcast_manager.drop_room(&room_name) {
                    error!(
                        "Failed to remove pre-created room from broadcast manager: {}",
                        e
                    );
                }

                // Get new room info
                let room_info_json: String = match redis_conn.get(&room_key).await {
                    Ok(info) => info,
                    Err(err) => {
                        error!("Failed to get room info from Redis after set_nx: {}", err);
                        return;
                    }
                };

                let room_info: RoomInfo = match serde_json::from_str(&room_info_json) {
                    Ok(info) => info,
                    Err(err) => {
                        error!("Failed to parse room info JSON: {}", err);
                        return;
                    }
                };

                debug!(
                    "Tried to build room '{}' but {} got to it first, building relay",
                    room_name, room_info.address
                );
                self.build_relay(socket, room_info.address.clone(), room_name.clone())
                    .await;
            }
        }
    }

    pub async fn build_relay(
        &self,
        socket: WebSocket,
        current_room_server: String,
        room_name: String,
    ) {
        let mut current_server_address = current_room_server;
        let mut socket = Some(socket);

        loop {
            let room_server_url = format!("ws://{}/{}", current_server_address, room_name);

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
                                trace!("Passing message {} to {}", msg, current_server_address);
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
                        let result = self.attempt_room_takeover(s, room_name.clone()).await;
                        match result {
                            Ok(new_socket) => {
                                // If takeover was successful, handle the socket directly
                                self.handle_socket(new_socket, room_name.clone()).await;
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
        &self,
        socket: WebSocket,
        room_name: String,
    ) -> Result<WebSocket, Option<(WebSocket, String)>> {
        info!("Attempting takeover of room {}", room_name);
        let room_key = RedisKeygenerator::room_key(&room_name);
        let mut redis_conn = match self.redis.get_multiplexed_async_connection().await {
            Ok(conn) => conn,
            Err(err) => {
                error!("Failed to get Redis connection: {}", err);
                return Err(None);
            }
        };

        // Get the current room info and TTL
        let room_info_json: Option<String> = match redis_conn.get(&room_key).await {
            Ok(info) => info,
            Err(err) => {
                error!("Failed to get room info from Redis: {}", err);
                return Err(None);
            }
        };

        let ttl: i32 = match redis_conn.ttl(&room_key).await {
            Ok(ttl) => ttl,
            Err(err) => {
                error!("Failed to get TTL from Redis: {}", err);
                return Err(None);
            }
        };

        // If the room info does not exist or TTL is expired, attempt to take over
        let can_takeover = room_info_json.is_none() || ttl <= 0;

        if can_takeover {
            // Try to set the room info in Redis with NX and TTL
            let room_info = RoomInfo {
                address: self.address.clone(),
                node_id: self.id.clone(),
                participants: self.broadcast_manager.listeners(&room_name),
            };

            // Serialize room_info to JSON
            let room_info_json = match serde_json::to_string(&room_info) {
                Ok(json) => json,
                Err(err) => {
                    error!("Failed to serialize room info: {}", err);
                    return Err(None);
                }
            };

            // Use a Lua script to set the room info with NX and TTL atomically
            let script = redis::Script::new(
                r#"
                local setnx = redis.call('SETNX', KEYS[1], ARGV[1])
                if setnx == 1 then
                    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
                end
                return setnx
                "#,
            );

            let ttl_seconds = 10;
            let set_result: i32 = match script
                .key(room_key.clone())
                .arg(&room_info_json)
                .arg(ttl_seconds)
                .invoke_async(&mut redis_conn)
                .await
            {
                Ok(result) => result,
                Err(err) => {
                    error!("Failed to set room info in Redis: {}", err);
                    return Err(None);
                }
            };

            if set_result == 1 {
                // Successfully took over the room

                // Start the room info worker to refresh TTL
                let (shutdown_tx, shutdown_rx) = watch::channel(());
                self.broadcast_manager
                    .store_room_shutdown_signal(&room_name, shutdown_tx)
                    .await;

                self.start_room_info_worker(room_name.clone(), shutdown_rx);

                Ok(socket)
            } else {
                // Another server took over the room; get the new server address
                let room_info_json: String = match redis_conn.get(&room_key).await {
                    Ok(info) => info,
                    Err(err) => {
                        error!("Failed to get room info from Redis after set_nx: {}", err);
                        return Err(None);
                    }
                };

                let room_info: RoomInfo = match serde_json::from_str(&room_info_json) {
                    Ok(info) => info,
                    Err(err) => {
                        error!("Failed to parse room info JSON: {}", err);
                        return Err(None);
                    }
                };

                Err(Some((socket, room_info.address)))
            }
        } else {
            // Another process updated the key; get the new server address
            let room_info_json = room_info_json.unwrap();
            let room_info: RoomInfo = match serde_json::from_str(&room_info_json) {
                Ok(info) => info,
                Err(err) => {
                    error!(
                        "Failed to parse room info JSON from Redis after DEL failed: {}",
                        err
                    );
                    return Err(None);
                }
            };
            Err(Some((socket, room_info.address)))
        }
    }

    pub async fn handle_socket(&self, socket: WebSocket, room_name: String) {
        let client_id = Arc::new(self.id_factory.gen_id());
        info!("Client {} connected to room '{}'", client_id, room_name);

        let room_key = RedisKeygenerator::room_key(&room_name);
        let mut redis_conn = match self.redis.get_multiplexed_async_connection().await {
            Ok(conn) => conn,
            Err(err) => {
                error!("Failed to get Redis connection: {}", err);
                return;
            }
        };

        // Split the WebSocket into sender and receiver
        let (ws_tx, ws_rx) = socket.split();

        // Prepare the sink to send binary messages
        let sink = ws_tx.with(|data: Vec<u8>| {
            futures::future::ok::<Message, axum::Error>(Message::Binary(data))
        });
        let sink = Arc::new(Mutex::new(sink));

        // Clone Arc to use inside the closure
        let client_id_clone = Arc::clone(&client_id);
        // Process incoming messages
        let stream = ws_rx
            .map(move |result| {
                let client_id = Arc::clone(&client_id_clone);
                result
                    .map_err(WebSocketError::ReceiveError)
                    .and_then(|msg| match msg {
                        Message::Binary(data) => {
                            trace!("Client {} sent binary data: {:?}", client_id, data);
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
        let subscription = match self
            .broadcast_manager
            .subscribe(&room_name, sink.clone(), stream)
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

        // Close the WebSocket connection
        if let Err(e) = sink.lock().await.close().await {
            error!("Failed to close WebSocket: {}", e);
        }

        // Unsubscribe the client when the connection is closed
        if let Err(e) = self.broadcast_manager.unsubscribe(&room_name) {
            error!("Client {} unsubscription error: {}", client_id, e);
        }

        // After unsubscribing, get the updated listener count
        if let Some(0) = self.broadcast_manager.listeners(&room_name) {
            // Attempt to drop the room
            match self.broadcast_manager.drop_room(&room_name) {
                Ok(_) => {
                    // Room was successfully dropped
                    debug!("Room '{}' dropped", room_name);

                    // Send shutdown signal to the room info worker
                    if let Some(shutdown_tx) = self
                        .broadcast_manager
                        .remove_room_shutdown_signal(&room_name)
                        .await
                    {
                        let _ = shutdown_tx.send(());
                    }

                    // Remove room info from Redis
                    match redis_conn.del::<&String, i32>(&room_key).await {
                        Ok(_) => {}
                        Err(e) => {
                            error!("Failed to drop room_key '{}' from Redis: {}", room_key, e);
                        }
                    }
                }
                Err(BroadcastManagerError::StillParticipants { .. }) => {
                    // Room still has participants; do not proceed
                    debug!("Room '{}' still has participants; not dropping", room_name);
                }
                Err(e) => {
                    error!("Failed to drop room '{}': {}", room_name, e);
                }
            }
        } else {
            // Room still has listeners or does not exist; do nothing
            debug!(
                "Room '{}' not dropped. Listeners: {:?}",
                room_name,
                self.broadcast_manager.listeners(&room_name)
            );
        }

        info!(
            "Client {} disconnected from room '{}'",
            client_id, room_name
        );
    }
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
