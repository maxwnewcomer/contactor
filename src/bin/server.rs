use axum::{routing::get, Router};
use redis::Client as RedisClient;
use std::env;
use std::sync::Arc;
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use yrs_relay::api::ws_handler;
use yrs_relay::api::AppState;
use yrs_relay::relay::RelayNode;

#[tokio::main]
async fn main() {
    // Initialize tracing subscriber
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("debug,tower_http=debug"))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load environment variables
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    let node_address = env::var("NODE_ADDRESS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());

    // Create Redis client
    let redis_client = RedisClient::open(redis_url).expect("Failed to create Redis client");

    let node = RelayNode::new(node_address, redis_client);

    // Create AppState
    let app_state = Arc::new(AppState {
        node: Arc::new(node),
    });

    // Build the application with the correct state
    let app = Router::new()
        .route("/:room_name", get(ws_handler))
        .with_state(app_state.clone())
        .layer(TraceLayer::new_for_http().make_span_with(DefaultMakeSpan::default()));

    let addr = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(addr, app.into_make_service()).await.unwrap();
}
