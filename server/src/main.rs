use axum::{routing::get, Router};
use config::Config;
use redis::Client as RedisClient;
use serde::Deserialize;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;
use tracing::warn;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use contactor::api::ws_handler;
use contactor::api::AppState;
use contactor::relay::RelayNode;

#[derive(Debug, Deserialize)]
struct AppConfig {
    redis_url: String,
    node_address: String,
}

#[tokio::main]
async fn main() {
    let _ = dotenvy::dotenv().map_err(|e| warn!("failed to load environment: {}", e));
    // Initialize tracing subscriber
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("debug,tower_http=debug"))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let env_reader = Config::builder()
        .add_source(config::Environment::default())
        .build()
        .unwrap();

    let config: AppConfig = env_reader
        .try_deserialize()
        .expect("failed to read environment");
    // Create Redis client
    let redis_client = RedisClient::open(config.redis_url).expect("Failed to create Redis client");

    let node = RelayNode::builder()
        .address(config.node_address)
        .redis(redis_client)
        .build()
        .expect("failed to build relay node");

    // Create AppState
    let app_state = Arc::new(AppState {
        node: Arc::new(node),
    });

    // Build the application with the correct state
    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/:room_name", get(ws_handler))
        .with_state(app_state.clone());

    let addr = TcpListener::bind("0.0.0.0:3000").await.unwrap();

    info!("Listening on 0.0.0.0:3000");

    axum::serve(addr, app.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
}

async fn shutdown_signal() {
    // Wait for the CTRL+C signal
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install CTRL+C signal handler");
    tracing::info!("Shutdown signal received");
}
