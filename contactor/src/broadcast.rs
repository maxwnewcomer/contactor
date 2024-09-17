//! Module responsible for managing broadcasting to multiple subscribers within rooms.

use anyhow::Result;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{watch, Mutex};
use yrs_warp::{
    broadcast::{BroadcastGroup, Subscription},
    AwarenessRef,
};

use crate::{DrainingState, RoomState, SyncingState};

/// The default capacity for the broadcast buffer.
const DEFAULT_BUFFER_CAPACITY: usize = 16;

/// Represents a chat room with a broadcast group and listener count.
pub struct Room {
    /// The broadcast group for the room.
    bcast: Arc<BroadcastGroup>,
    /// The number of listeners connected to the room.
    listeners: AtomicUsize,
    /// The current state of the room
    pub(crate) state: Arc<Mutex<RoomState>>,
}

/// Errors that can occur when managing broadcasts.
#[derive(Debug, Error)]
pub enum BroadcastManagerError {
    /// The specified room was not found.
    #[error("Room '{room_name}' not found")]
    RoomNotFound { room_name: String },

    /// The specified room already exists.
    #[error("Room '{room_name}' already exists")]
    RoomAlreadyExists { room_name: String },

    /// The room still has participants connected.
    #[error("Room '{room_name}' still has participants")]
    StillParticipants { room_name: String },

    /// Underflow occurred when decrementing listener count.
    #[error("Listener count underflow in room '{room_name}'")]
    ListenerCountUnderflow { room_name: String },
}

/// Manages broadcasting messages to subscribers across multiple rooms.
pub struct BroadcastManager {
    /// A thread-safe map of room names to their corresponding `Room` instances.
    pub(crate) rooms: DashMap<String, Room>,
    /// A mutex-protected map for room shutdown signals.
    room_shutdown_signals: Mutex<HashMap<String, watch::Sender<()>>>,
}

impl Debug for BroadcastManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BroadcastManager")
            // .field("rooms", &self.rooms)
            // .field("room_shutdown_signals", &self.room_shutdown_signals)
            .finish()
    }
}

impl BroadcastManager {
    const MAX_SYNC_RETRIES: u32 = 3;
    const MAX_DRAIN_RETRIES: u32 = 3;

    /// Creates a new `BroadcastManager`.
    pub fn new() -> Self {
        Self {
            rooms: DashMap::new(),
            room_shutdown_signals: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the number of active rooms.
    pub fn num_rooms(&self) -> usize {
        self.rooms.len()
    }

    /// Returns the total number of listeners across all rooms.
    pub fn total_listeners(&self) -> usize {
        self.rooms
            .iter()
            .map(|entry| entry.value().listeners.load(Ordering::SeqCst))
            .sum()
    }

    /// Returns the number of listeners in a specific room.
    ///
    /// # Arguments
    ///
    /// * `room_name` - The name of the room.
    pub fn listeners(&self, room_name: &str) -> Option<usize> {
        self.rooms
            .get(room_name)
            .map(|room| room.listeners.load(Ordering::Relaxed))
    }

    pub fn get_room(&self, room_name: &str) -> Option<dashmap::mapref::one::Ref<String, Room>> {
        self.rooms.get(room_name)
    }

    /// Subscribes a client to a room's broadcast group.
    ///
    /// # Type Parameters
    ///
    /// * `Sink` - The sink type for sending messages.
    /// * `Stream` - The stream type for receiving messages.
    /// * `E` - The error type for the stream.
    ///
    /// # Arguments
    ///
    /// * `room_name` - The name of the room to subscribe to.
    /// * `sink` - An `Arc<Mutex<Sink>>` for sending messages to the client.
    /// * `stream` - A stream of incoming messages from the client.
    ///
    /// # Errors
    ///
    /// Returns `BroadcastManagerError::RoomNotFound` if the room does not exist.
    pub fn subscribe<Sink, Stream, E>(
        &self,
        room_name: &str,
        sink: Arc<Mutex<Sink>>,
        stream: Stream,
    ) -> Result<Subscription, BroadcastManagerError>
    where
        Sink: SinkExt<Vec<u8>> + Send + Sync + Unpin + 'static,
        Stream: StreamExt<Item = Result<Vec<u8>, E>> + Send + Sync + Unpin + 'static,
        Sink::Error: std::error::Error + Send + Sync,
        E: std::error::Error + Send + Sync + 'static,
    {
        let room = self
            .rooms
            .get(room_name)
            .ok_or(BroadcastManagerError::RoomNotFound {
                room_name: room_name.to_string(),
            })?;
        room.listeners.fetch_add(1, Ordering::SeqCst);
        Ok(room.bcast.subscribe(sink, stream))
    }

    /// Attempts to remove a room if it has no participants.
    ///
    /// # Arguments
    ///
    /// * `room_name` - The name of the room to drop.
    ///
    /// # Errors
    ///
    /// Returns an error if the room still has participants.
    pub fn drop_room(&self, room_name: &str) -> Result<(), BroadcastManagerError> {
        let removed = self.rooms.remove_if(room_name, |_, room| {
            room.listeners.load(Ordering::SeqCst) == 0
        });

        match removed {
            Some(_) => Ok(()), // Room was successfully removed
            None => {
                if self.rooms.contains_key(room_name) {
                    Err(BroadcastManagerError::StillParticipants {
                        room_name: room_name.to_string(),
                    })
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Unsubscribes a client from a room.
    ///
    /// # Arguments
    ///
    /// * `room_name` - The name of the room.
    ///
    /// # Errors
    ///
    /// Returns an error if the room is not found or underflow occurs.
    pub fn unsubscribe(&self, room_name: &str) -> Result<(), BroadcastManagerError> {
        if let Some(room_ref) = self.rooms.get(room_name) {
            let prev_count = room_ref.listeners.fetch_sub(1, Ordering::SeqCst);
            if prev_count == 0 {
                room_ref.listeners.fetch_add(1, Ordering::SeqCst); // Revert decrement
                return Err(BroadcastManagerError::ListenerCountUnderflow {
                    room_name: room_name.to_string(),
                });
            }
            Ok(())
        } else {
            Err(BroadcastManagerError::RoomNotFound {
                room_name: room_name.to_string(),
            })
        }
    }

    /// Creates a new room with the specified name and awareness.
    ///
    /// # Arguments
    ///
    /// * `room_name` - The name of the room to create.
    /// * `awareness` - An `AwarenessRef` for the room.
    ///
    /// # Errors
    ///
    /// Returns an error if the room already exists.
    pub async fn create_room(
        &self,
        room_name: &str,
        awareness: AwarenessRef,
    ) -> Result<(), BroadcastManagerError> {
        if self.rooms.contains_key(room_name) {
            return Err(BroadcastManagerError::RoomAlreadyExists {
                room_name: room_name.to_string(),
            });
        }
        let room = Room {
            bcast: Arc::new(BroadcastGroup::new(awareness, DEFAULT_BUFFER_CAPACITY).await),
            listeners: AtomicUsize::new(0),
            state: Arc::new(Mutex::new(RoomState::Down)),
        };
        self.rooms.insert(room_name.to_string(), room);

        // Start syncing process
        self.start_syncing(room_name).await;

        Ok(())
    }

    /// Stores a shutdown signal sender for a room.
    ///
    /// # Arguments
    ///
    /// * `room_name` - The name of the room.
    /// * `shutdown_tx` - A watch channel sender for shutdown signals.
    pub async fn store_room_shutdown_signal(
        &self,
        room_name: &str,
        shutdown_tx: watch::Sender<()>,
    ) {
        let mut signals = self.room_shutdown_signals.lock().await;
        signals.insert(room_name.to_string(), shutdown_tx);
    }

    /// Removes the shutdown signal sender for a room.
    ///
    /// # Arguments
    ///
    /// * `room_name` - The name of the room.
    ///
    /// # Returns
    ///
    /// An optional `watch::Sender<()>` if it exists.
    pub async fn remove_room_shutdown_signal(&self, room_name: &str) -> Option<watch::Sender<()>> {
        let mut signals = self.room_shutdown_signals.lock().await;
        signals.remove(room_name)
    }

    pub async fn start_syncing(&self, room_name: &str) {
        let room = match self.rooms.get(room_name) {
            Some(room) => room,
            None => return, // Room doesn't exist
        };

        // Clone for async move
        let state = room.state.clone();
        let room_name = room_name.to_string();

        tokio::spawn(async move {
            let mut retry_count = 0;

            loop {
                {
                    let mut state_guard = state.lock().await;
                    *state_guard = RoomState::Syncing(SyncingState::Load(retry_count));
                }

                // Attempt to load data (replace with your actual load logic)
                match attempt_load(&room_name).await {
                    Ok(_) => {
                        let mut state_guard = state.lock().await;
                        *state_guard = RoomState::Syncing(SyncingState::Success);
                        break;
                    }
                    Err(_) if retry_count < Self::MAX_SYNC_RETRIES => {
                        retry_count += 1;
                        {
                            let mut state_guard = state.lock().await;
                            *state_guard = RoomState::Syncing(SyncingState::RetryLoad(retry_count));
                        }
                        // Wait before retrying
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                    Err(_) => {
                        let mut state_guard = state.lock().await;
                        *state_guard = RoomState::Syncing(SyncingState::Fail);
                        // Handle failure (e.g., log, alert)
                        return;
                    }
                }
            }

            // Transition to UP state
            {
                let mut state_guard = state.lock().await;
                *state_guard = RoomState::Up;
            }
        });
    }

    pub async fn start_draining(self: Arc<Self>, room_name: &str) {
        let room = match self.rooms.get(room_name) {
            Some(room) => room,
            None => return, // Room doesn't exist
        };

        let state = room.state.clone();
        let room_name = room_name.to_string();
        let self_clone = self.clone();
        let max_retries = Self::MAX_DRAIN_RETRIES;

        tokio::spawn(async move {
            let mut retry_count = 0;

            {
                let mut state_guard = state.lock().await;
                *state_guard = RoomState::Draining(DrainingState::Store(retry_count));
            }

            loop {
                // Attempt to store data (replace with your actual store logic)
                match attempt_store(&room_name).await {
                    Ok(_) => {
                        let mut state_guard = state.lock().await;
                        *state_guard = RoomState::Draining(DrainingState::Success);
                        break;
                    }
                    Err(_) if retry_count < max_retries => {
                        retry_count += 1;
                        {
                            let mut state_guard = state.lock().await;
                            *state_guard =
                                RoomState::Draining(DrainingState::RetryStore(retry_count));
                        }
                        // Wait before retrying
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                    Err(_) => {
                        let mut state_guard = state.lock().await;
                        *state_guard = RoomState::Draining(DrainingState::Fail);
                        // Handle failure (e.g., log, alert)
                        return;
                    }
                }
            }

            // Transition to DOWN state and remove room
            {
                let mut state_guard = state.lock().await;
                *state_guard = RoomState::Down;
            }

            // Remove room from manager
            self_clone.rooms.remove(&room_name);
        });
    }
}

// Mock function to simulate storing
async fn attempt_load(_room_name: &str) -> Result<(), ()> {
    // Implement your actual data storing logic here
    Ok(())
}

// Mock function to simulate storing
async fn attempt_store(_room_name: &str) -> Result<(), ()> {
    // Implement your actual data storing logic here
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::mpsc;
    use futures::future::join_all;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::io;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{Mutex, RwLock};
    use yrs::sync::Awareness;
    use yrs::Doc;

    // Helper function to create a mock AwarenessRef
    fn create_mock_awareness() -> AwarenessRef {
        Arc::new(RwLock::new(Awareness::new(Doc::new())))
    }

    // Helper function to create a mock stream
    fn create_mock_stream() -> impl StreamExt<Item = Result<Vec<u8>, io::Error>> {
        futures::stream::empty()
    }

    #[tokio::test]
    async fn test_create_room() {
        let manager = BroadcastManager::new();
        let awareness = create_mock_awareness();

        // Test creating a new room
        assert!(manager
            .create_room("room1", awareness.clone())
            .await
            .is_ok());

        // Test creating a room that already exists
        let result = manager.create_room("room1", awareness).await;
        assert!(matches!(
            result,
            Err(BroadcastManagerError::RoomAlreadyExists { .. })
        ));
    }

    #[tokio::test]
    async fn test_subscribe_and_unsubscribe() {
        let manager = BroadcastManager::new();
        let awareness = create_mock_awareness();

        // Create a room
        manager.create_room("room1", awareness).await.unwrap();

        // Create a mock sink and stream
        let (tx, _rx) = mpsc::channel(10);
        let sink = Arc::new(Mutex::new(tx));
        let stream = create_mock_stream();

        // Test subscribing
        let subscription = manager.subscribe("room1", sink.clone(), stream);
        assert!(subscription.is_ok());

        // Test unsubscribing
        assert!(manager.unsubscribe("room1").is_ok());

        // Test unsubscribing from a non-existent room
        let result = manager.unsubscribe("non_existent_room");
        assert!(matches!(
            result,
            Err(BroadcastManagerError::RoomNotFound { .. })
        ));
    }

    #[tokio::test]
    async fn test_drop_room() {
        let manager = BroadcastManager::new();
        let awareness = create_mock_awareness();

        // Create a room
        manager.create_room("room1", awareness).await.unwrap();

        // Test dropping an empty room
        assert!(manager.drop_room("room1").is_ok());

        // Test dropping a non-existent room
        assert!(manager.drop_room("non_existent_room").is_ok());

        // Create a new room and subscribe to it
        manager
            .create_room("room2", create_mock_awareness())
            .await
            .unwrap();
        let (tx, _rx) = mpsc::channel(10);
        let sink = Arc::new(Mutex::new(tx));
        let stream = create_mock_stream();
        manager.subscribe("room2", sink, stream).unwrap();

        // Test dropping a room with active listeners
        let result = manager.drop_room("room2");
        assert!(matches!(
            result,
            Err(BroadcastManagerError::StillParticipants { .. })
        ));
    }

    #[tokio::test]
    async fn test_multiple_subscriptions() {
        let manager = BroadcastManager::new();
        let awareness = create_mock_awareness();

        // Create a room
        manager.create_room("room1", awareness).await.unwrap();

        // Create multiple subscriptions
        for _ in 0..3 {
            let (tx, _rx) = mpsc::channel(10);
            let sink = Arc::new(Mutex::new(tx));
            let stream = create_mock_stream();
            manager.subscribe("room1", sink, stream).unwrap();
        }

        // Try to drop the room (should fail due to active listeners)
        let result = manager.drop_room("room1");
        assert!(matches!(
            result,
            Err(BroadcastManagerError::StillParticipants { .. })
        ));

        // Unsubscribe all
        for _ in 0..3 {
            manager.unsubscribe("room1").unwrap();
        }

        // Now dropping should succeed
        assert!(manager.drop_room("room1").is_ok());
    }

    #[tokio::test]
    async fn test_complex_concurrent_operations() {
        let manager = Arc::new(BroadcastManager::new());
        let num_rooms = 30;
        let num_operations_per_room = 100;

        // Create rooms
        for i in 0..num_rooms {
            let awareness = create_mock_awareness();
            manager
                .create_room(&format!("room-{}", i), awareness)
                .await
                .unwrap();
        }

        // Spawn tasks for each room
        let mut tasks = vec![];
        for room_id in 0..num_rooms {
            let manager_clone = manager.clone();
            let task = tokio::spawn(async move {
                // Create a thread-safe RNG
                let mut rng = StdRng::from_entropy();
                let room_name = format!("room-{}", room_id);

                for _ in 0..num_operations_per_room {
                    let operation = rng.gen_range(0..4);
                    match operation {
                        0 => {
                            // Subscribe
                            let (tx, _rx) = mpsc::channel(10);
                            let sink = Arc::new(Mutex::new(tx));
                            let stream = create_mock_stream();
                            let _ = manager_clone.subscribe(&room_name, sink, stream);
                        }
                        1 => {
                            // Unsubscribe
                            let _ = manager_clone.unsubscribe(&room_name);
                        }
                        2 => {
                            // Attempt to drop
                            let _ = manager_clone.drop_room(&room_name);
                        }
                        3 => {
                            // Simulate some processing time
                            tokio::time::sleep(Duration::from_millis(rng.gen_range(1..10))).await;
                        }
                        _ => unreachable!(),
                    }
                }
            });
            tasks.push(task);
        }

        // Wait for all tasks to complete
        let results = join_all(tasks).await;

        // Check if any task panicked
        for result in results {
            assert!(result.is_ok());
        }

        // Verify final state
        for i in 0..num_rooms {
            let room_name = format!("room-{}", i);
            let result = manager.drop_room(&room_name);
            match result {
                Ok(()) => {
                    // Room was successfully dropped, which means it had no listeners
                    println!("Room {} was successfully dropped", room_name);
                }
                Err(BroadcastManagerError::StillParticipants { .. }) => {
                    // Room still has listeners
                    println!("Room {} still has listeners", room_name);
                }
                Err(BroadcastManagerError::RoomNotFound { .. }) => {
                    // Room was already dropped during the test
                    println!("Room {} was already dropped", room_name);
                }
                _ => panic!("Unexpected error for room {}", room_name),
            }
        }
    }
}
