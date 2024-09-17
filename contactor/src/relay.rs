//! Module handling WebSocket connections and relaying messages between clients and rooms.

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::extract::ws::{Message, WebSocket};
use bon::bon;
use futures::{FutureExt, SinkExt, StreamExt};
use redis::AsyncCommands;
use sysinfo::System;
use thiserror::Error;
use tokio::sync::{watch, Mutex, RwLock};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message as TungsteniteMessage;
use tracing::{debug, error, info, trace};
use yrs::sync::Awareness;
use yrs::Doc;

use crate::broadcast::{BroadcastManager, BroadcastManagerError};
use crate::ids::IdFactory;
use crate::{DrainingState, NodeInfo, RedisKeygenerator, RoomInfo, RoomState, SyncingState};

/// Represents a relay node responsible for handling client connections and room management.
pub struct RelayNode {
    /// The network address of the relay node.
    pub address: String,

    /// The unique identifier of the relay node.
    pub id: String,

    /// Redis client for interacting with the Redis server.
    redis: redis::Client,

    /// Manages broadcasting to clients across rooms.
    broadcast_manager: Arc<BroadcastManager>,

    /// Factory for generating unique IDs.
    id_factory: Arc<IdFactory>,
}

impl Debug for RelayNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelayNode")
            .field("address", &self.address)
            .field("id", &self.id)
            .finish()
    }
}

#[bon]
impl RelayNode {
    #[builder(on(String, into))]
    /// Creates a new `RelayNode` and starts the node info worker.
    ///
    /// # Arguments
    ///
    /// * `address` - The network address of the relay node.
    /// * `redis` - A Redis client.
    pub fn new(
        address: String,
        redis: redis::Client,
        id_factory: Option<IdFactory>,
        id: Option<String>,
        broadcast_manager: Option<BroadcastManager>,
    ) -> Self {
        let factory = id_factory.unwrap_or(IdFactory::new());
        let id = id.unwrap_or(factory.gen_id());
        let broadcast_manager = broadcast_manager.unwrap_or(BroadcastManager::new());

        let relay_node = RelayNode {
            address: address.clone(),
            id: id,
            redis: redis.clone(),
            broadcast_manager: Arc::new(broadcast_manager),
            id_factory: Arc::new(factory),
        };

        // Start the node info worker
        relay_node.start_node_info_worker();

        relay_node
    }

    /// Starts a background task that periodically reports node info to Redis.
    #[tracing::instrument]
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

    /// Starts a background task that periodically reports room info to Redis.
    ///
    /// # Arguments
    ///
    /// * `room_name` - The name of the room.
    /// * `shutdown_rx` - A receiver for shutdown signals.
    #[tracing::instrument]
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

                let room_state = {
                    let room = broadcast_clone.rooms.get(&room_name);
                    if let Some(room) = room {
                        let state_guard = room.state.lock().await;
                        state_guard.clone()
                    } else {
                        RoomState::Down // Default or handle as needed
                    }
                };

                // Create the room info to report
                let room_info = RoomInfo {
                    address: address_clone.clone(),
                    node_id: id.clone(),
                    participants: broadcast_clone.listeners(&room_name),
                    status: room_state, // Include the state
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
                trace!("Room worker updated room info for '{}'", room_name);
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

    /// Handles a WebSocket upgrade by routing the connection to the appropriate room or building a relay.
    ///
    /// # Arguments
    ///
    /// * `socket` - The WebSocket connection.
    /// * `room_name` - The name of the room to connect to.
    #[tracing::instrument(skip(socket))]
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
                match room_info.status {
                    RoomState::Up => {
                        // Room is ready; handle the socket
                        debug!("Handling connection to '{}' locally", room_name);
                        self.handle_socket(socket, room_name.clone()).await;
                    }
                    RoomState::Syncing(_) => {
                        // Room is syncing; wait until it's up
                        debug!(
                            "Room '{}' is syncing. Waiting for it to become Up",
                            room_name
                        );
                        self.wait_for_room_to_be_up(socket, room_name.clone()).await;
                    }
                    _ => {
                        // Other states: attempt to take over or handle accordingly
                        debug!(
                            "Room '{}' is in state {:?}. Attempting takeover.",
                            room_name, room_info.status
                        );
                        match self.attempt_room_takeover(socket, room_name.clone()).await {
                            Ok(new_socket) => {
                                self.handle_socket(new_socket, room_name.clone()).await;
                            }
                            Err(Some((new_socket, new_server_address))) => {
                                // Another server took over; build relay to new server
                                self.build_relay(new_socket, new_server_address, room_name.clone())
                                    .await;
                            }
                            Err(None) => {
                                // Failed to take over and no new server address; cannot proceed
                                error!("Failed to take over room '{}'", room_name);
                            }
                        }
                    }
                }
            } else {
                // Room is on a different server
                match room_info.status {
                    RoomState::Up => {
                        // Build relay to the room's server
                        debug!(
                            "Expecting room '{}' to be hosted at {}, building relay",
                            room_name, room_info.address
                        );
                        self.build_relay(socket, room_info.address.clone(), room_name.clone())
                            .await;
                    }
                    _ => {
                        // Room is not up; attempt to take over
                        debug!(
                            "Room '{}' is in state {:?} on server {}. Attempting takeover.",
                            room_name, room_info.status, room_info.address
                        );
                        match self.attempt_room_takeover(socket, room_name.clone()).await {
                            Ok(new_socket) => {
                                self.handle_socket(new_socket, room_name.clone()).await;
                            }
                            Err(Some((new_socket, new_server_address))) => {
                                // Another server took over; build relay to new server
                                self.build_relay(new_socket, new_server_address, room_name.clone())
                                    .await;
                            }
                            Err(None) => {
                                // Failed to take over and no new server address; cannot proceed
                                error!("Failed to take over room '{}'", room_name);
                            }
                        }
                    }
                }
            }
        } else {
            // Room does not exist; attempt to create it
            debug!(
                "Room '{}' does not exist. Attempting to create it.",
                room_name
            );
            match self.create_room_and_set_info(&room_name).await {
                Ok(()) => {
                    // Now, handle the socket
                    self.handle_socket(socket, room_name.clone()).await;
                }
                Err(_) => {
                    // Another server created the room; get updated room info and proceed
                    let room_info_json: Option<String> = match redis_conn.get(&room_key).await {
                        Ok(info) => info,
                        Err(err) => {
                            error!("Failed to get room info from Redis after set_nx: {}", err);
                            return;
                        }
                    };
                    if let Some(room_info_json) = room_info_json {
                        let room_info: RoomInfo = match serde_json::from_str(&room_info_json) {
                            Ok(info) => info,
                            Err(err) => {
                                error!("Failed to parse room info JSON: {}", err);
                                return;
                            }
                        };

                        if room_info.address == self.address {
                            // Room is hosted on this server now
                            self.handle_socket(socket, room_name.clone()).await;
                        } else {
                            // Build relay to new server
                            self.build_relay(socket, room_info.address.clone(), room_name.clone())
                                .await;
                        }
                    } else {
                        // No room info; cannot proceed
                        error!("Failed to retrieve updated room info for '{}'", room_name);
                    }
                }
            }
        }
    }

    async fn create_room_and_set_info(&self, room_name: &str) -> Result<(), String> {
        // Create room locally
        match self
            .broadcast_manager
            .create_room(room_name, Arc::new(RwLock::new(Awareness::new(Doc::new()))))
            .await
        {
            Ok(_) => {}
            Err(e) => {
                let err_msg = format!(
                    "Failed to create room '{}' in broadcast manager: {}",
                    room_name,
                    e.to_string()
                );
                error!("{}", err_msg);
                return Err(err_msg);
            }
        }

        // Try to set the room info in Redis with NX and TTL
        let room_info = RoomInfo {
            address: self.address.clone(),
            node_id: self.id.clone(),
            participants: self.broadcast_manager.listeners(room_name),
            status: RoomState::Down, // Room is in Down state initially
        };

        // Serialize room_info to JSON
        let room_info_json = match serde_json::to_string(&room_info) {
            Ok(json) => json,
            Err(err) => {
                let err_msg = format!("Failed to serialize room info: {}", err);
                error!("{}", err_msg);
                return Err(err_msg);
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
        let room_key = RedisKeygenerator::room_key(room_name);
        let mut redis_conn = match self.redis.get_multiplexed_async_connection().await {
            Ok(conn) => conn,
            Err(err) => {
                let err_msg = format!("Failed to get Redis connection: {}", err);
                error!("{}", err_msg);
                return Err(err_msg);
            }
        };

        let set_result: i32 = match script
            .key(&room_key)
            .arg(&room_info_json)
            .arg(ttl_seconds)
            .invoke_async(&mut redis_conn)
            .await
        {
            Ok(result) => result,
            Err(err) => {
                let err_msg = format!("Failed to set room info in Redis: {}", err);
                error!("{}", err_msg);
                return Err(err_msg);
            }
        };

        if set_result == 1 {
            // Successfully created the room in Redis
            debug!("Room '{}' didn't exist so I created it", room_name);

            // Start the room info worker to refresh TTL
            let (shutdown_tx, shutdown_rx) = watch::channel(());
            self.broadcast_manager
                .store_room_shutdown_signal(room_name, shutdown_tx)
                .await;

            self.start_room_info_worker(room_name.to_string(), shutdown_rx);

            // Start syncing process
            self.broadcast_manager.start_syncing(room_name).await;

            Ok(())
        } else {
            // Another server created the room; get the updated room info

            // Tear down local room
            if let Err(e) = self.broadcast_manager.drop_room(room_name) {
                error!(
                    "Failed to remove pre-created room from broadcast manager: {}",
                    e
                );
            }

            Err("Room already exists".to_string())
        }
    }

    fn can_take_over(&self, room_info: &RoomInfo, ttl: i32) -> bool {
        match room_info.status {
            RoomState::Down => true,
            RoomState::Syncing(SyncingState::Fail) => true,
            RoomState::Draining(DrainingState::Fail) => true,
            _ => ttl <= 0,
        }
    }

    /// Builds a relay between the client and the current room server.
    ///
    /// # Arguments
    ///
    /// * `socket` - The WebSocket connection from the client.
    /// * `current_room_server` - The address of the current room server.
    /// * `room_name` - The name of the room.
    #[tracing::instrument(skip(socket))]
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

    /// Attempts to take over a room if the current server is unresponsive.
    ///
    /// # Arguments
    ///
    /// * `socket` - The WebSocket connection from the client.
    /// * `room_name` - The name of the room.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or an `Option` containing the socket and new server address.
    #[tracing::instrument(skip(socket))]
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

        let can_takeover = match &room_info_json {
            Some(json) => {
                let room_info: RoomInfo = match serde_json::from_str(&json) {
                    Ok(info) => info,
                    Err(err) => {
                        error!("Failed to parse room info JSON: {}", err);
                        return Err(None);
                    }
                };
                self.can_take_over(&room_info, ttl)
            }
            None => true, // No room info, can attempt to take over
        };

        if can_takeover {
            // Proceed to take over the room
            match self.create_room_and_set_info(&room_name).await {
                Ok(()) => Ok(socket),
                Err(_) => {
                    // Another server took over; get updated room info
                    let room_info_json: Option<String> = match redis_conn.get(&room_key).await {
                        Ok(info) => info,
                        Err(err) => {
                            error!("Failed to get room info from Redis after set_nx: {}", err);
                            return Err(None);
                        }
                    };
                    if let Some(room_info_json) = room_info_json {
                        let room_info: RoomInfo = match serde_json::from_str(&room_info_json) {
                            Ok(info) => info,
                            Err(err) => {
                                error!("Failed to parse room info JSON: {}", err);
                                return Err(None);
                            }
                        };

                        Err(Some((socket, room_info.address)))
                    } else {
                        // No room info; cannot proceed
                        error!("Failed to retrieve updated room info for '{}'", room_name);
                        Err(None)
                    }
                }
            }
        } else {
            // Cannot take over, return Err with updated room address
            let room_info_json = room_info_json.unwrap();
            let room_info: RoomInfo = match serde_json::from_str(&room_info_json) {
                Ok(info) => info,
                Err(err) => {
                    error!("Failed to parse room info JSON: {}", err);
                    return Err(None);
                }
            };
            Err(Some((socket, room_info.address)))
        }
    }

    async fn wait_for_room_to_be_up(&self, socket: WebSocket, room_name: String) {
        let room = match self.broadcast_manager.rooms.get(&room_name) {
            Some(room) => room,
            None => {
                error!(
                    "Room '{}' not found while waiting for it to become Up",
                    room_name
                );
                return;
            }
        };

        let wait_duration = Duration::from_secs(10); // Adjust as needed

        if let Ok(_) = timeout(wait_duration, async {
            loop {
                let state = {
                    let state_guard = room.state.lock().await;
                    state_guard.clone()
                };

                match state {
                    RoomState::Up => {
                        // Now handle the socket
                        self.handle_socket(socket, room_name.clone()).await;
                        break;
                    }
                    RoomState::Syncing(_) => {
                        // Still syncing; wait and check again
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    _ => {
                        // Room transitioned to a different state; break the loop
                        break;
                    }
                }
            }
        })
        .await
        {
            // Successfully waited for room to be Up
        } else {
            // Timeout occurred
            error!(
                "Timeout while waiting for room '{}' to become Up",
                room_name
            );
            // Handle timeout (e.g., attempt takeover or inform the client)
        }
    }

    /// Handles the WebSocket connection for a specific room.
    ///
    /// # Arguments
    ///
    /// * `socket` - The WebSocket connection.
    /// * `room_name` - The name of the room.
    #[tracing::instrument(skip(socket))]
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

/// Errors that can occur when handling WebSocket messages.
#[derive(Debug, Error)]
enum WebSocketError {
    /// An error occurred while receiving a WebSocket message.
    #[error("WebSocket receive error: {0}")]
    ReceiveError(#[from] axum::Error),

    /// An unsupported message type was received.
    #[error("Unsupported message type")]
    UnsupportedMessageType,
}

unsafe impl Send for WebSocketError {}
unsafe impl Sync for WebSocketError {}
