//! Module responsible for managing broadcasting to multiple subscribers within rooms.

use anyhow::Result;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{watch, Mutex};
use yrs_warp::{
    broadcast::{BroadcastGroup, Subscription},
    AwarenessRef,
};

/// The default capacity for the broadcast buffer.
const DEFAULT_BUFFER_CAPACITY: usize = 16;

/// Represents a chat room with a broadcast group and listener count.
struct Room {
    /// The broadcast group for the room.
    bcast: Arc<BroadcastGroup>,
    /// The number of listeners connected to the room.
    listeners: AtomicUsize,
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
    rooms: DashMap<String, Room>,
    /// A mutex-protected map for room shutdown signals.
    room_shutdown_signals: Mutex<HashMap<String, watch::Sender<()>>>,
}

impl BroadcastManager {
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
        };
        self.rooms.insert(room_name.to_string(), room);
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
}
