use anyhow::Result;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
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
}

impl BroadcastManager {
    pub fn new() -> Self {
        Self {
            rooms: DashMap::new(),
        }
    }

    pub async fn subscribe<Sink, Stream, E>(
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

    pub async fn drop_room(&self, room_name: &str) -> Result<(), BroadcastManagerError> {
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

    pub async fn unsubscribe(&self, room_name: &str) -> Result<(), BroadcastManagerError> {
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

    pub async fn get_awareness(
        &self,
        room_name: &str,
    ) -> Result<AwarenessRef, BroadcastManagerError> {
        if let Some(room) = self.rooms.get(room_name) {
            Ok(room.bcast.awareness().clone())
        } else {
            Err(BroadcastManagerError::RoomNotFound {
                room_name: room_name.to_string(),
            })
        }
    }
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
        let subscription = manager.subscribe("room1", sink.clone(), stream).await;
        assert!(subscription.is_ok());

        // Test unsubscribing
        assert!(manager.unsubscribe("room1").await.is_ok());

        // Test unsubscribing from a non-existent room
        let result = manager.unsubscribe("non_existent_room").await;
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
        assert!(manager.drop_room("room1").await.is_ok());

        // Test dropping a non-existent room
        assert!(manager.drop_room("non_existent_room").await.is_ok());

        // Create a new room and subscribe to it
        manager
            .create_room("room2", create_mock_awareness())
            .await
            .unwrap();
        let (tx, _rx) = mpsc::channel(10);
        let sink = Arc::new(Mutex::new(tx));
        let stream = create_mock_stream();
        manager.subscribe("room2", sink, stream).await.unwrap();

        // Test dropping a room with active listeners
        let result = manager.drop_room("room2").await;
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
            manager.subscribe("room1", sink, stream).await.unwrap();
        }

        // Try to drop the room (should fail due to active listeners)
        let result = manager.drop_room("room1").await;
        assert!(matches!(
            result,
            Err(BroadcastManagerError::StillParticipants { .. })
        ));

        // Unsubscribe all
        for _ in 0..3 {
            manager.unsubscribe("room1").await.unwrap();
        }

        // Now dropping should succeed
        assert!(manager.drop_room("room1").await.is_ok());
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
                            let _ = manager_clone.subscribe(&room_name, sink, stream).await;
                        }
                        1 => {
                            // Unsubscribe
                            let _ = manager_clone.unsubscribe(&room_name).await;
                        }
                        2 => {
                            // Attempt to drop
                            let _ = manager_clone.drop_room(&room_name).await;
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
            let result = manager.drop_room(&room_name).await;
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
