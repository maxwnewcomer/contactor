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

const DEFAULT_BUFFER_CAPACITY: usize = 16;

struct Room {
    bcast: Arc<BroadcastGroup>,
    listeners: AtomicUsize,
}

#[derive(Debug, Error)]
pub enum BroadcastManagerError {
    #[error("Room '{room_name}' not found")]
    RoomNotFound { room_name: String },
    #[error("Room '{room_name}' already exists")]
    RoomAlreadyExists { room_name: String },
    #[error("Room '{room_name}' still has participants")]
    StillParticipants { room_name: String },
    #[error("Listener count underflow in room '{room_name}'")]
    ListenerCountUnderflow { room_name: String },
}

pub struct BroadcastManager {
    rooms: DashMap<String, Room>,
    room_shutdown_signals: Mutex<HashMap<String, watch::Sender<()>>>,
}

impl BroadcastManager {
    pub fn new() -> Self {
        Self {
            rooms: DashMap::new(),
            room_shutdown_signals: Mutex::new(HashMap::new()),
        }
    }

    pub fn num_rooms(&self) -> usize {
        self.rooms.len()
    }

    pub fn total_listeners(&self) -> usize {
        self.rooms
            .iter()
            .map(|entry| entry.value().listeners.load(Ordering::SeqCst))
            .sum()
    }

    pub fn listeners(&self, room_name: &str) -> Option<usize> {
        self.rooms
            .get(room_name)
            .map(|room| room.listeners.load(Ordering::Relaxed))
    }

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

    pub async fn store_room_shutdown_signal(
        &self,
        room_name: &str,
        shutdown_tx: watch::Sender<()>,
    ) {
        let mut signals = self.room_shutdown_signals.lock().await;
        signals.insert(room_name.to_string(), shutdown_tx);
    }

    pub async fn remove_room_shutdown_signal(&self, room_name: &str) -> Option<watch::Sender<()>> {
        let mut signals = self.room_shutdown_signals.lock().await;
        signals.remove(room_name)
    }
}
