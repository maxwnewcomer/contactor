//! Main library module for the crate.
//!
//! This module re-exports the `api` and `relay` modules and includes internal utilities.

pub mod api;
pub mod relay;

pub(crate) mod broadcast;
pub(crate) mod ids;

/// A utility struct for generating Redis keys for rooms and nodes.
pub(crate) struct RedisKeygenerator {}

impl RedisKeygenerator {
    /// Generates a Redis key for a given room name.
    ///
    /// # Arguments
    ///
    /// * `room_name` - A string slice that holds the name of the room.
    ///
    /// # Returns
    ///
    /// A `String` representing the Redis key for the room.
    pub fn room_key(room_name: &str) -> String {
        format!("room:{}", room_name)
    }

    /// Generates a Redis key for a given node ID.
    ///
    /// # Arguments
    ///
    /// * `node_id` - A string slice that holds the ID of the node.
    ///
    /// # Returns
    ///
    /// A `String` representing the Redis key for the node.
    pub fn node_key(node_id: &str) -> String {
        format!("node:{}", node_id)
    }
}
