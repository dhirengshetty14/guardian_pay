use std::{env, net::SocketAddr, sync::Arc};

use anyhow::Context;
use axum::{
    extract::{Path, State, WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use axum::extract::ws::{Message, WebSocket};
use chrono::Utc;
use common_models::{DecisionTraceV1, TransactionEventV1};
use futures::StreamExt;
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    Message,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::broadcast;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{error, info};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    producer: FutureProducer,
    tx_raw_topic: String,
    alert_sender: broadcast::Sender<DecisionTraceV1>,
    simulator_url: String,
    analytics_url: String,
    client: reqwest::Client,
}

#[derive(Debug, Deserialize)]
struct ScorePreviewRequest {
    account_id: String,
    card_id: String,
    merchant_id: String,
    amount: f64,
    currency: String,
    country: String,
    device_id: String,
    ip: String,
    mcc: String,
    channel: String,
    is_card_present: bool,
    user_agent: String,
}

#[derive(Debug, Serialize)]
struct ScorePreviewResponse {
    tx_id: String,
    status: String,
    queued_at: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct SimulationRunRequest {
    scenario: String,
    count: Option<u32>,
    rate_per_sec: Option<u32>,
}

#[derive(Debug, Serialize)]
struct BasicResponse {
    status: String,
    message: String,
}

#[derive(Debug, Deserialize)]
struct ReplayRunRequest {
    from_offset: Option<i64>,
    to_offset: Option<i64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            env::var("RUST_LOG")
                .unwrap_or_else(|_| "info,gateway_api=debug,rdkafka=warn".to_string()),
        )
        .init();

    let kafka_brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let tx_raw_topic = env::var("TOPIC_TX_RAW").unwrap_or_else(|_| "tx.raw".to_string());
    let tx_decision_topic =
        env::var("TOPIC_TX_DECISION").unwrap_or_else(|_| "tx.decision".to_string());
    let simulator_url =
        env::var("SIMULATOR_URL").unwrap_or_else(|_| "http://simulator:8090".to_string());
    let analytics_url =
        env::var("ANALYTICS_URL").unwrap_or_else(|_| "http://analytics-writer:8083".to_string());

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .context("failed to create Kafka producer")?;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "gateway-alerts")
        .set("bootstrap.servers", &kafka_brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .create()
        .context("failed to create decision consumer")?;
    consumer
        .subscribe(&[&tx_decision_topic])
        .context("failed to subscribe tx.decision")?;

    let (alert_sender, _) = broadcast::channel(1024);

    tokio::spawn(run_alert_fanout(consumer, alert_sender.clone()));

    let state = Arc::new(AppState {
        producer,
        tx_raw_topic,
        alert_sender,
        simulator_url,
        analytics_url,
        client: reqwest::Client::new(),
    });

    let app = Router::new()
        .route("/v1/health", get(health))
        .route("/v1/transactions/score-preview", post(score_preview))
        .route("/v1/simulations/run", post(run_simulation))
        .route("/v1/transactions/:tx_id/decision", get(get_decision))
        .route("/v1/rings/:ring_id", get(get_ring))
        .route("/v1/replay/run", post(run_replay))
        .route("/v1/stream/alerts", get(stream_alerts))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let bind_addr: SocketAddr = env::var("GATEWAY_BIND")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse()
        .context("invalid GATEWAY_BIND")?;

    info!("gateway-api listening on {bind_addr}");
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn run_alert_fanout(consumer: StreamConsumer, sender: broadcast::Sender<DecisionTraceV1>) {
    let mut stream = consumer.stream();
    while let Some(message_result) = stream.next().await {
        match message_result {
            Ok(msg) => {
                if let Some(payload) = msg.payload() {
                    match serde_json::from_slice::<DecisionTraceV1>(payload) {
                        Ok(decision) => {
                            let _ = sender.send(decision);
                        }
                        Err(err) => {
                            error!("failed to decode tx.decision payload: {err}");
                        }
                    }
                }
            }
            Err(err) => error!("Kafka consume error in gateway alert fanout: {err}"),
        }
    }
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok",
        "service": "gateway-api",
        "timestamp": Utc::now()
    }))
}

async fn score_preview(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ScorePreviewRequest>,
) -> Result<Json<ScorePreviewResponse>, (StatusCode, String)> {
    let tx_id = Uuid::new_v4().to_string();
    let tx = TransactionEventV1 {
        tx_id: tx_id.clone(),
        event_time: Utc::now(),
        account_id: payload.account_id,
        card_id: payload.card_id,
        merchant_id: payload.merchant_id,
        amount: payload.amount,
        currency: payload.currency,
        country: payload.country,
        device_id: payload.device_id,
        ip: payload.ip,
        mcc: payload.mcc,
        channel: payload.channel,
        is_card_present: payload.is_card_present,
        user_agent: payload.user_agent,
        label: None,
    };

    let body = serde_json::to_string(&tx).map_err(internal_error)?;
    state
        .producer
        .send(
            FutureRecord::to(&state.tx_raw_topic)
                .key(&tx_id)
                .payload(&body),
            Timeout::After(std::time::Duration::from_secs(3)),
        )
        .await
        .map_err(|(err, _)| {
            (
                StatusCode::BAD_GATEWAY,
                format!("failed to publish tx.raw: {err}"),
            )
        })?;

    Ok(Json(ScorePreviewResponse {
        tx_id,
        status: "queued".to_string(),
        queued_at: Utc::now().to_rfc3339(),
    }))
}

async fn run_simulation(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<SimulationRunRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let url = format!("{}/internal/run", state.simulator_url);
    let response = state
        .client
        .post(url)
        .json(&payload)
        .send()
        .await
        .map_err(|err| {
            (
                StatusCode::BAD_GATEWAY,
                format!("simulator call failed: {err}"),
            )
        })?;

    if !response.status().is_success() {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("simulator returned {}", response.status()),
        ));
    }

    let body = response.json::<serde_json::Value>().await.map_err(internal_error)?;
    Ok(Json(body))
}

async fn get_decision(
    State(state): State<Arc<AppState>>,
    Path(tx_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let url = format!("{}/internal/decisions/{}", state.analytics_url, tx_id);
    proxy_get_json(&state.client, &url).await
}

async fn get_ring(
    State(state): State<Arc<AppState>>,
    Path(ring_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let url = format!("{}/internal/rings/{}", state.analytics_url, ring_id);
    proxy_get_json(&state.client, &url).await
}

async fn run_replay(
    Json(payload): Json<ReplayRunRequest>,
) -> Result<Json<BasicResponse>, (StatusCode, String)> {
    Ok(Json(BasicResponse {
        status: "accepted".to_string(),
        message: format!(
            "Replay requested for offsets {:?}..{:?}. Run replay-cli to execute deterministic comparison.",
            payload.from_offset, payload.to_offset
        ),
    }))
}

async fn stream_alerts(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> Response {
    ws.on_upgrade(move |socket| ws_handler(socket, state.alert_sender.subscribe()))
}

async fn ws_handler(mut socket: WebSocket, mut rx: broadcast::Receiver<DecisionTraceV1>) {
    while let Ok(alert) = rx.recv().await {
        let payload = match serde_json::to_string(&alert) {
            Ok(body) => body,
            Err(err) => {
                error!("failed to serialize websocket alert: {err}");
                continue;
            }
        };
        if socket.send(Message::Text(payload)).await.is_err() {
            break;
        }
    }
}

async fn proxy_get_json(
    client: &reqwest::Client,
    url: &str,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|err| (StatusCode::BAD_GATEWAY, format!("proxy request failed: {err}")))?;
    let status = response.status();
    let body = response
        .json::<serde_json::Value>()
        .await
        .unwrap_or_else(|_| json!({"status": "error", "message": "upstream returned invalid json"}));
    if !status.is_success() {
        return Err((StatusCode::BAD_GATEWAY, body.to_string()));
    }
    Ok(Json(body))
}

fn internal_error(err: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
