use axum::{routing::get, Router};
use std::sync::Arc; // Import Arc for shared state
use tokio::sync::RwLock;
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use yrs::sync::Awareness;
use yrs::Doc;

use yrs_relay::api::ws_handler;
use yrs_relay::broadcast::BroadcastManager;

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=debug,tower_http=debug", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let broadcast_manager = Arc::new(BroadcastManager::new());
    let _ = broadcast_manager
        .create_room(
            &"default",
            Arc::new(RwLock::new(Awareness::new(Doc::new()))),
        )
        .await;

    let app = Router::new()
        .route("/:room_name", get(ws_handler))
        .with_state(broadcast_manager.clone())
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::default().include_headers(true)),
        );

    let addr = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(addr, app.into_make_service()).await.unwrap();
}
