use crate::{NodeInfo, RoomInfo};
use anyhow::Result;
use async_trait::async_trait;
use redis::AsyncCommands;
use std::fmt::Debug;

#[async_trait]
pub trait StorageBackend: Send + Sync + Debug {
    async fn set_node_info(&self, node_id: &str, info: NodeInfo, ttl_secs: u64) -> Result<()>;
    async fn set_room_info(&self, room_name: &str, info: RoomInfo, ttl_secs: u64) -> Result<()>;
    async fn get_room_info(&self, room_name: &str) -> Result<Option<RoomInfo>>;
    async fn get_room_ttl(&self, room_name: &str) -> Result<i32>;
    async fn set_room_info_nx(
        &self,
        room_name: &str,
        info: RoomInfo,
        ttl_secs: u64,
    ) -> Result<bool>;
    async fn delete_room_info(&self, room_name: &str) -> Result<()>;
    async fn set_room_info_if_not_exists(
        &self,
        key: &str,
        info: RoomInfo,
        ttl: i32,
    ) -> Result<bool>;
}

#[derive(Debug)]
pub struct RedisStorage {
    client: redis::Client,
}

impl RedisStorage {
    pub fn new(client: redis::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl StorageBackend for RedisStorage {
    async fn set_node_info(&self, node_id: &str, info: NodeInfo, ttl_secs: u64) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let node_key = crate::RedisKeygenerator::node_key(node_id);
        conn.set_ex::<_, _, ()>(&node_key, info, ttl_secs).await?;
        Ok(())
    }

    async fn set_room_info(&self, room_name: &str, info: RoomInfo, ttl_secs: u64) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let room_key = crate::RedisKeygenerator::room_key(room_name);
        conn.set_ex::<_, _, ()>(&room_key, info, ttl_secs).await?;
        Ok(())
    }

    async fn get_room_info(&self, room_name: &str) -> Result<Option<RoomInfo>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let room_key = crate::RedisKeygenerator::room_key(room_name);
        let info: Option<RoomInfo> = conn.get(&room_key).await?;
        Ok(info)
    }

    async fn get_room_ttl(&self, room_name: &str) -> Result<i32> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let room_key = crate::RedisKeygenerator::room_key(room_name);
        let ttl = conn.ttl(&room_key).await?;
        Ok(ttl)
    }

    async fn set_room_info_nx(
        &self,
        room_name: &str,
        info: RoomInfo,
        ttl_secs: u64,
    ) -> Result<bool> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let room_key = crate::RedisKeygenerator::room_key(room_name);

        // Use a Lua script to set NX with TTL atomically
        let script = redis::Script::new(
            r#"
            local setnx = redis.call('SETNX', KEYS[1], ARGV[1])
            if setnx == 1 then
                redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
            end
            return setnx
            "#,
        );

        let result: i32 = script
            .key(&room_key)
            .arg(info)
            .arg(ttl_secs)
            .invoke_async(&mut conn)
            .await?;

        Ok(result == 1)
    }

    async fn delete_room_info(&self, room_name: &str) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let room_key = crate::RedisKeygenerator::room_key(room_name);
        conn.del(&room_key).await?;
        Ok(())
    }

    async fn set_room_info_if_not_exists(
        &self,
        key: &str,
        info: RoomInfo,
        ttl: i32,
    ) -> Result<bool> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let room_key = crate::RedisKeygenerator::room_key(key);

        let script = redis::Script::new(
            r#"
            local setnx = redis.call('SETNX', KEYS[1], ARGV[1])
            if setnx == 1 then
                redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
            end
            return setnx
            "#,
        );

        let result: i32 = script
            .key(&room_key)
            .arg(serde_json::to_string(&info)?)
            .arg(ttl)
            .invoke_async(&mut conn)
            .await?;

        Ok(result == 1)
    }
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::sync::RwLock;

    #[derive(Debug, Clone)]
    struct StorageEntry {
        data: Vec<u8>,
        expires_at: Option<Instant>,
    }

    #[derive(Debug)]
    pub struct MockStorage {
        store: Arc<RwLock<HashMap<String, StorageEntry>>>,
    }

    impl MockStorage {
        pub fn new() -> Self {
            Self {
                store: Arc::new(RwLock::new(HashMap::new())),
            }
        }
    }

    #[async_trait]
    impl StorageBackend for MockStorage {
        async fn set_node_info(&self, node_id: &str, info: NodeInfo, ttl_secs: u64) -> Result<()> {
            let mut store = self.store.write().await;
            let key = crate::RedisKeygenerator::node_key(node_id);
            let expires_at = Some(Instant::now() + Duration::from_secs(ttl_secs));
            store.insert(
                key,
                StorageEntry {
                    data: serde_json::to_vec(&info)?,
                    expires_at,
                },
            );
            Ok(())
        }

        async fn set_room_info(
            &self,
            room_name: &str,
            info: RoomInfo,
            ttl_secs: u64,
        ) -> Result<()> {
            let mut store = self.store.write().await;
            let key = crate::RedisKeygenerator::room_key(room_name);
            let expires_at = Some(Instant::now() + Duration::from_secs(ttl_secs));
            store.insert(
                key,
                StorageEntry {
                    data: serde_json::to_vec(&info)?,
                    expires_at,
                },
            );
            Ok(())
        }

        async fn get_room_info(&self, room_name: &str) -> Result<Option<RoomInfo>> {
            let store = self.store.read().await;
            let key = crate::RedisKeygenerator::room_key(room_name);

            if let Some(entry) = store.get(&key) {
                if let Some(expires_at) = entry.expires_at {
                    if expires_at <= Instant::now() {
                        return Ok(None);
                    }
                }
                let info: RoomInfo = serde_json::from_slice(&entry.data)?;
                Ok(Some(info))
            } else {
                Ok(None)
            }
        }

        async fn get_room_ttl(&self, room_name: &str) -> Result<i32> {
            let store = self.store.read().await;
            let key = crate::RedisKeygenerator::room_key(room_name);

            if let Some(entry) = store.get(&key) {
                if let Some(expires_at) = entry.expires_at {
                    let now = Instant::now();
                    if expires_at <= now {
                        Ok(-2)
                    } else {
                        Ok((expires_at - now).as_secs() as i32)
                    }
                } else {
                    Ok(-1)
                }
            } else {
                Ok(-2)
            }
        }

        async fn set_room_info_nx(
            &self,
            room_name: &str,
            info: RoomInfo,
            ttl_secs: u64,
        ) -> Result<bool> {
            let mut store = self.store.write().await;
            let key = crate::RedisKeygenerator::room_key(room_name);

            if !store.contains_key(&key) {
                store.insert(
                    key,
                    StorageEntry {
                        data: serde_json::to_vec(&info)?,
                        expires_at: Some(Instant::now() + Duration::from_secs(ttl_secs)),
                    },
                );
                Ok(true)
            } else {
                Ok(false)
            }
        }

        async fn delete_room_info(&self, room_name: &str) -> Result<()> {
            let mut store = self.store.write().await;
            let key = crate::RedisKeygenerator::room_key(room_name);
            store.remove(&key);
            Ok(())
        }

        async fn set_room_info_if_not_exists(
            &self,
            key: &str,
            info: RoomInfo,
            ttl: i32,
        ) -> Result<bool> {
            let mut store = self.store.write().await;
            let room_key = crate::RedisKeygenerator::room_key(key);

            if !store.contains_key(&room_key) {
                store.insert(
                    room_key,
                    StorageEntry {
                        data: serde_json::to_vec(&info)?,
                        expires_at: Some(Instant::now() + Duration::from_secs(ttl as u64)),
                    },
                );
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }
}
