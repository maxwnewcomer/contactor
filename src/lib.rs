pub mod api;
pub mod relay;

pub(crate) mod broadcast;
pub(crate) mod ids;

pub(crate) struct RedisKeygenerator {}

impl RedisKeygenerator {
    pub fn room_key(room_name: &str) -> String {
        format!("room:{}", room_name)
    }

    pub fn node_key(node_id: &str) -> String {
        format!("node:{}", node_id)
    }
}
