use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::State,
    http::{Response, header::CONTENT_TYPE},
    response::IntoResponse,
    routing::get,
};
use prometheus_client::{encoding::text::encode, registry::Registry};
use tokio::sync::Mutex;
use tracing::info;
use tracing_subscriber::fmt::{format::Writer, time::FormatTime};

pub mod config;
pub mod mq;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_timer(LocalTimer).init();
    info!("start flink-kafka-fliter-transform...");
    let registry_original = Arc::new(Mutex::new(Registry::default()));

    let registry = registry_original.clone();
    tokio::spawn(async move {
        mq::consumer_kafka(registry).await;
    });

    let registry = registry_original.clone();
    let app = Router::new()
        .route("/version", get(version))
        .route("/metrics", get(metrics_handler))
        .with_state(registry);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:9266").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn version() -> &'static str {
    "1.0.0"
}

async fn metrics_handler(State(state): State<Arc<Mutex<Registry>>>) -> impl IntoResponse {
    let state = state.lock().await;
    let mut buffer = String::new();
    encode(&mut buffer, &state).unwrap();
    return Response::builder()
        .header(
            CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )
        .body(Body::from(buffer))
        .unwrap();
}

struct LocalTimer;

const fn east_utf8() -> Option<chrono::FixedOffset> {
    chrono::FixedOffset::east_opt(8 * 3600)
}

impl FormatTime for LocalTimer {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        let now = chrono::Utc::now().with_timezone(&east_utf8().unwrap());
        write!(w, "{}", now.format("%FT%T%.3f"))
    }
}
