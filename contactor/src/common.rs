use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// Struct for serializing node information.
#[derive(Serialize)]
pub struct NodeInfo {
    pub address: String,
    pub num_rooms: usize,
    pub num_connections: usize,
    pub cpu_usage: f32,
    pub total_memory: u64,
    pub used_memory: u64,
}

/// Struct for serializing and deserializing room information.
#[derive(Serialize, Deserialize)]
pub struct RoomInfo {
    pub address: String,
    pub node_id: String,
    pub participants: Option<usize>,
    pub status: RoomState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoomState {
    Down,
    Syncing(SyncingState),
    Up,
    Draining(DrainingState),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncingState {
    Load(u32),
    RetryLoad(u32),
    Success,
    Fail,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainingState {
    Store(u32),
    RetryStore(u32),
    Success,
    Fail,
}

impl Serialize for RoomState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = match self {
            RoomState::Down => "DOWN".to_string(),
            RoomState::Syncing(sync_state) => match sync_state {
                SyncingState::Load(_) => "SYNCING_LOAD".to_string(),
                SyncingState::RetryLoad(_) => "SYNCING_RETRY_LOAD".to_string(),
                SyncingState::Success => "SYNCING_SUCCESS".to_string(),
                SyncingState::Fail => "SYNCING_FAIL".to_string(),
            },
            RoomState::Up => "UP".to_string(),
            RoomState::Draining(draining_state) => match draining_state {
                DrainingState::Store(_) => "DRAINING_STORE".to_string(),
                DrainingState::RetryStore(_) => "DRAINING_RETRY_STORE".to_string(),
                DrainingState::Success => "DRAINING_SUCCESS".to_string(),
                DrainingState::Fail => "DRAINING_FAIL".to_string(),
            },
        };
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for RoomState {
    fn deserialize<D>(deserializer: D) -> Result<RoomState, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RoomStateVisitor;

        impl<'de> Visitor<'de> for RoomStateVisitor {
            type Value = RoomState;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a SCREAMING_SNAKE_CASE string representing RoomState")
            }

            fn visit_str<E>(self, value: &str) -> Result<RoomState, E>
            where
                E: de::Error,
            {
                match value {
                    "DOWN" => Ok(RoomState::Down),
                    "SYNCING_LOAD" => Ok(RoomState::Syncing(SyncingState::Load(0))), // Default value; adjust as needed
                    "SYNCING_RETRY_LOAD" => Ok(RoomState::Syncing(SyncingState::RetryLoad(0))),
                    "SYNCING_SUCCESS" => Ok(RoomState::Syncing(SyncingState::Success)),
                    "SYNCING_FAIL" => Ok(RoomState::Syncing(SyncingState::Fail)),
                    "UP" => Ok(RoomState::Up),
                    "DRAINING_STORE" => Ok(RoomState::Draining(DrainingState::Store(0))),
                    "DRAINING_RETRY_STORE" => Ok(RoomState::Draining(DrainingState::RetryStore(0))),
                    "DRAINING_SUCCESS" => Ok(RoomState::Draining(DrainingState::Success)),
                    "DRAINING_FAIL" => Ok(RoomState::Draining(DrainingState::Fail)),
                    _ => Err(de::Error::unknown_variant(
                        value,
                        &[
                            "DOWN",
                            "SYNCING_LOAD",
                            "SYNCING_RETRY_LOAD",
                            "SYNCING_SUCCESS",
                            "SYNCING_FAIL",
                            "UP",
                            "DRAINING_STORE",
                            "DRAINING_RETRY_STORE",
                            "DRAINING_SUCCESS",
                            "DRAINING_FAIL",
                        ],
                    )),
                }
            }
        }

        deserializer.deserialize_str(RoomStateVisitor)
    }
}
