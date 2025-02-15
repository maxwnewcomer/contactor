//! Module handling WebSocket connections and relaying messages between clients and rooms.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::extract::ws::{Message, WebSocket};
use bon::bon;
use futures::future::join_all;
use futures::{FutureExt, SinkExt, StreamExt};
use sysinfo::System;
use thiserror::Error;
use tokio::sync::broadcast;
use tokio::sync::{watch, Mutex, RwLock};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message as TungsteniteMessage;
use tracing::{debug, error, info, trace};
use yrs::sync::Awareness;
use yrs::Doc;

use crate::broadcast::{BroadcastManager, BroadcastManagerError};
use crate::ids::IdFactory;
use crate::storage::StorageBackend;
use crate::{DrainingState, NodeInfo, RedisKeygenerator, RoomInfo, RoomState, SyncingState};

/// Represents a relay node responsible for handling client connections and room management.
pub struct RelayNode {
    /// The network address of the relay node.
    pub address: String,
    /// The unique identifier of the relay node.
    pub id: String,
    /// Storage backend for interacting with the storage server.
    storage: Arc<dyn StorageBackend>,
    /// Manages broadcasting to clients across rooms.
    broadcast_manager: Arc<BroadcastManager>,
    /// Factory for generating unique IDs.
    id_factory: Arc<IdFactory>,
    /// Tracks active connections to other nodes.
    node_connections: Arc<Mutex<HashMap<String, SharedNodeConnection>>>,
}

impl Debug for RelayNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelayNode")
            .field("address", &self.address)
            .field("id", &self.id)
            .finish()
    }
}

/// Represents a shared connection to another node.
struct SharedNodeConnection {
    sender: broadcast::Sender<TungsteniteMessage>,
    ref_count: usize,
}

#[bon]
impl RelayNode {
    #[builder(on(String, into))]
    /// Creates a new `RelayNode` and starts the node info worker.
    ///
    /// # Arguments
    ///
    /// * `address` - The network address of the relay node.
    /// * `storage` - A storage backend.
    pub fn new(
        address: String,
        storage: Arc<dyn StorageBackend>,
        id_factory: Option<IdFactory>,
        id: Option<String>,
        broadcast_manager: Option<BroadcastManager>,
    ) -> Self {
        let factory = id_factory.unwrap_or(IdFactory::new());
        let id = id.unwrap_or(factory.gen_id());
        let broadcast_manager = broadcast_manager.unwrap_or(BroadcastManager::default());

        let relay_node = RelayNode {
            address: address.clone(),
            id,
            storage,
            broadcast_manager: Arc::new(broadcast_manager),
            id_factory: Arc::new(factory),
            node_connections: Arc::new(Mutex::new(HashMap::new())),
        };

        // Start the node info worker
        relay_node.start_node_info_worker();

        relay_node
    }

    /// Starts a background task that periodically reports node info to Redis.
    #[tracing::instrument]
    fn start_node_info_worker(&self) {
        let storage_clone = self.storage.clone();
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
                let node_info_key = RedisKeygenerator::node_key(&id);

                // Store in Redis with an expiry
                let result = storage_clone
                    .set_node_info(&node_info_key, node_info, 10)
                    .await;
                if let Err(err) = result {
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
        let storage_clone = self.storage.clone();
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
                let result = storage_clone.set_room_info(&room_key, room_info, 10).await;
                if let Err(err) = result {
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
        let room_info: Option<RoomInfo> = self.storage.get_room_info(&room_key).await.unwrap();

        if let Some(room_info) = room_info {
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
                    let room_info: Option<RoomInfo> =
                        self.storage.get_room_info(&room_key).await.unwrap();
                    if let Some(room_info) = room_info {
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
        let room_key = RedisKeygenerator::room_key(room_name);

        let (room_info, ttl) = tokio::try_join!(
            self.storage.get_room_info(&room_key),
            self.storage.get_room_ttl(&room_key)
        )
        .map_err(|e| format!("Failed to get room info or TTL: {}", e))?;

        let can_takeover = match &room_info {
            Some(room_info) => self.can_take_over(room_info, ttl),
            None => true,
        };

        if can_takeover {
            self.do_create_room(room_name).await
        } else {
            let room_info = room_info.ok_or_else(|| "Room info missing".to_string())?;
            Err(format!(
                "Room is not in a takeoverable state: {:?}",
                room_info.status
            ))
        }
    }

    async fn do_create_room(&self, room_name: &str) -> Result<(), String> {
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

        // Try to set the room info in Redis
        let room_info = RoomInfo {
            address: self.address.clone(),
            node_id: self.id.clone(),
            participants: self.broadcast_manager.listeners(room_name),
            status: RoomState::Down,
        };

        let room_key = RedisKeygenerator::room_key(room_name);
        match self
            .storage
            .set_room_info_if_not_exists(&room_key, room_info, 10)
            .await
        {
            Ok(true) => {
                debug!("Room '{}' didn't exist so I created it", room_name);

                // Start the room info worker
                let (shutdown_tx, shutdown_rx) = watch::channel(());
                self.broadcast_manager
                    .store_room_shutdown_signal(room_name, shutdown_tx)
                    .await;

                self.start_room_info_worker(room_name.to_string(), shutdown_rx);

                // Start syncing process
                self.broadcast_manager.start_syncing(room_name).await;
                Ok(())
            }
            Ok(false) => {
                // Another server created the room
                if let Err(e) = self.broadcast_manager.drop_room(room_name) {
                    error!(
                        "Failed to remove pre-created room from broadcast manager: {}",
                        e
                    );
                }
                let room_info = self
                    .storage
                    .get_room_info(&room_key)
                    .await
                    .map_err(|e| format!("Failed to get updated room info: {}", e))?;

                match room_info {
                    Some(info) => Err(format!("Room already exists: {}", info.address)),
                    None => Err("Room info not found".to_string()),
                }
            }
            Err(e) => Err(format!("Redis error: {}", e)),
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
            match self
                .get_or_create_node_connection(&current_server_address, &room_name)
                .await
            {
                Ok((sender, mut room_receiver)) => {
                    let (mut client_ws_sink, mut client_ws_stream) = socket.take().unwrap().split();

                    let sender_clone = sender.clone();
                    let current_server_address_clone = current_server_address.clone();
                    let client_to_room = async move {
                        while let Some(msg) = client_ws_stream.next().await {
                            if let Ok(msg) = msg {
                                let tung_msg = match msg {
                                    Message::Text(text) => TungsteniteMessage::Text(text),
                                    Message::Binary(bin) => TungsteniteMessage::Binary(bin),
                                    _ => continue,
                                };
                                trace!(
                                    "Passing message {:?} to {}",
                                    tung_msg,
                                    current_server_address_clone
                                );
                                if sender_clone.send(tung_msg).is_err() {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                    };

                    let room_to_client = async move {
                        while let Ok(msg) = room_receiver.recv().await {
                            let axum_msg = match msg {
                                TungsteniteMessage::Text(text) => Message::Text(text),
                                TungsteniteMessage::Binary(bin) => Message::Binary(bin),
                                _ => continue,
                            };
                            if client_ws_sink.send(axum_msg).await.is_err() {
                                break;
                            }
                        }
                    };

                    let server_addr = current_server_address.clone();
                    let self_clone = self.clone();
                    tokio::spawn(async move {
                        // Run both directions concurrently.
                        futures::future::select(client_to_room.boxed(), room_to_client.boxed())
                            .await;
                        // Instead of manually modifying the map, call release_node_connection.
                        self_clone.release_node_connection(&server_addr).await;
                    });

                    break;
                }
                Err(err) => {
                    error!(
                        "Failed to connect to room server at {}: {}",
                        current_server_address, err
                    );
                    // Attempt takeover logic remains the same...
                    if let Some(s) = socket.take() {
                        let result = self.attempt_room_takeover(s, room_name.clone()).await;
                        match result {
                            Ok(new_socket) => {
                                self.handle_socket(new_socket, room_name.clone()).await;
                                break;
                            }
                            Err(Some((new_socket, new_server_address))) => {
                                socket = Some(new_socket);
                                current_server_address = new_server_address;
                                continue;
                            }
                            Err(None) => break,
                        }
                    } else {
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

        // Get room info and TTL in parallel using tokio::try_join!
        let (room_info, ttl) = tokio::try_join!(
            self.storage.get_room_info(&room_key),
            self.storage.get_room_ttl(&room_key)
        )
        .map_err(|e| {
            error!("Failed to get room info or TTL: {}", e);
            None
        })?;

        let can_takeover = match &room_info {
            Some(room_info) => self.can_take_over(room_info, ttl),
            None => true,
        };

        if can_takeover {
            match self.create_room_and_set_info(&room_name).await {
                Ok(()) => Ok(socket),
                Err(_) => {
                    let room_info = self.storage.get_room_info(&room_key).await.map_err(|e| {
                        error!("Failed to get updated room info: {}", e);
                        None
                    })?;

                    match room_info {
                        Some(info) => Err(Some((socket, info.address))),
                        None => {
                            error!("Failed to retrieve updated room info for '{}'", room_name);
                            Err(None)
                        }
                    }
                }
            }
        } else {
            let room_info = room_info.ok_or(None)?;
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
            return ();
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
        let room_key = RedisKeygenerator::room_key(&room_name);

        info!("Client {} connected to room '{}'", client_id, room_name);

        // Clone for use inside the closure
        let client_id_for_stream = client_id.clone();

        // Split the WebSocket into sender and receiver...
        let (ws_tx, ws_rx) = socket.split();

        // Prepare the sink...
        let sink = ws_tx.with(|data: Vec<u8>| {
            futures::future::ok::<Message, axum::Error>(Message::Binary(data))
        });
        let sink = Arc::new(Mutex::new(sink));

        // Use the clone inside the stream mapping closure:
        let stream = ws_rx
            .map(move |result| {
                result
                    .map_err(WebSocketError::ReceiveError)
                    .and_then(|msg| match msg {
                        Message::Binary(data) => {
                            trace!(
                                "Client {} sent binary data: {:?}",
                                client_id_for_stream,
                                data
                            );
                            Ok(data)
                        }
                        Message::Text(text) => {
                            trace!("Client {} sent text data: {}", client_id_for_stream, text);
                            Ok(text.into_bytes())
                        }
                        _ => Err(WebSocketError::UnsupportedMessageType),
                    })
            })
            .take_while(|res| futures::future::ready(res.is_ok()));

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
                    let result = self.storage.delete_room_info(&room_key).await;
                    if let Err(e) = result {
                        error!("Failed to drop room_key '{}' from Redis: {}", room_key, e);
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

    pub async fn start_drain(&self) {
        let futures = self.broadcast_manager.rooms.iter().map(|room| {
            let room_key = room.key().clone();
            let manager = self.broadcast_manager.clone();
            debug!("Starting drain for '{}'", &room_key);
            async move {
                manager.start_draining(&room_key).await;
                manager
                    .drop_room(&room_key)
                    .map(|_| room_key.clone())
                    .map_err(|e| (e, room_key.clone()))
            }
        });

        // Use `join_all` to await all the futures concurrently
        let _ = join_all(futures)
            .await
            .iter()
            .for_each(|drain_res| match drain_res {
                Ok(room) => {
                    debug!("Successfully drained `{}`", room)
                }
                Err((e, room)) => {
                    error!("Failed to drain {}: {}", room, e)
                }
            });
    }

    /// Adds a new connection to the node connections map.
    ///
    /// # Arguments
    ///
    /// * `server_address` - The address of the server to connect to.
    /// * `room_name` - The name of the room to connect to.
    ///
    /// # Returns
    ///
    /// A tuple of the broadcast sender and receiver for the connection.
    async fn get_or_create_node_connection(
        &self,
        server_address: &str,
        room_name: &str,
    ) -> Result<
        (
            broadcast::Sender<TungsteniteMessage>,
            broadcast::Receiver<TungsteniteMessage>,
        ),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let mut connections = self.node_connections.lock().await;

        if let Some(connection) = connections.get_mut(server_address) {
            connection.ref_count += 1;
            debug!(
                "Reusing connection to {}. Ref count increased to {}",
                server_address, connection.ref_count
            );
            Ok((connection.sender.clone(), connection.sender.subscribe()))
        } else {
            let room_server_url = format!("ws://{}/{}", server_address, room_name);
            let (ws_stream, _) = connect_async(&room_server_url).await?;

            let (sender, _) = broadcast::channel(100);
            let connection = SharedNodeConnection {
                sender: sender.clone(),
                ref_count: 1,
            };

            // Clone `server_address` for use in the spawned task.
            let server_addr = server_address.to_owned();
            let sender_clone = sender.clone();
            tokio::spawn(async move {
                let (_write, mut read) = ws_stream.split();
                while let Some(message) = read.next().await {
                    match message {
                        Ok(msg) => {
                            let _ = sender_clone.send(msg);
                        }
                        Err(e) => {
                            error!("Error in ws_stream from {}: {}", server_addr, e);
                            break;
                        }
                    }
                }
            });

            connections.insert(server_address.to_string(), connection);
            Ok((sender.clone(), sender.subscribe()))
        }
    }

    /// Releases a connection from the node connections map.
    ///
    /// # Arguments
    ///
    /// * `server_address` - The address of the server to release the connection from.
    async fn release_node_connection(&self, server_address: &str) {
        let mut connections = self.node_connections.lock().await;

        if let Some(connection) = connections.get_mut(server_address) {
            connection.ref_count = connection.ref_count.saturating_sub(1);
            debug!(
                "Released connection to {}. New ref_count: {}",
                server_address, connection.ref_count
            );
            if connection.ref_count == 0 {
                debug!(
                    "No more references to connection to {}. Removing connection.",
                    server_address
                );
                connections.remove(server_address);
            }
        } else {
            debug!(
                "Attempted to release non-existing connection to {}",
                server_address
            );
        }
    }
}

impl Clone for RelayNode {
    fn clone(&self) -> Self {
        Self {
            address: self.address.clone(),
            id: self.id.clone(),
            storage: self.storage.clone(),
            broadcast_manager: self.broadcast_manager.clone(),
            id_factory: self.id_factory.clone(),
            node_connections: self.node_connections.clone(),
        }
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
