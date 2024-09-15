use std::sync::Arc;

use axum::{
    extract::{ws::WebSocketUpgrade, Path, State},
    response::IntoResponse,
};

use crate::relay::RelayNode;

pub struct AppState {
    pub node: Arc<RelayNode>,
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    Path(room_name): Path<String>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| {
        let node = state.node.clone();
        let room_name = room_name.clone();
        async move {
            node.handle_upgrade(socket, room_name).await;
        }
    })
}
