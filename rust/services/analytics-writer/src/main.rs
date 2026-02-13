use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use chrono::Utc;
use clickhouse::{Client as ClickHouseClient, Row};
use common_models::{DecisionTraceV1, GraphSignalV1};
use futures::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{config::ClientConfig, Message};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::mpsc;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{error, info, warn};

#[derive(Clone)]
struct AppState {
    clickhouse: ClickHouseClient,
    db_name: String,
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
struct DecisionRow {
    tx_id: String,
    decision: String,
    final_score: f64,
    policy_version: String,
    model_version: String,
    replay_hash: String,
    latency_ms: u64,
    reasons_json: String,
    label: String,
    event_time: String,
    inserted_at: String,
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
struct RingRow {
    ring_id: String,
    component_size: u32,
    avg_graph_score: f64,
    shared_device_fanout: u32,
    updated_at: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();

    let kafka_brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let topic_tx_decision =
        env::var("TOPIC_TX_DECISION").unwrap_or_else(|_| "tx.decision".to_string());
    let topic_tx_graph = env::var("TOPIC_TX_GRAPH").unwrap_or_else(|_| "tx.graph".to_string());
    let kafka_group_id =
        env::var("KAFKA_GROUP_ID").unwrap_or_else(|_| "analytics-writer-v2".to_string());
    let kafka_auto_offset_reset =
        env::var("KAFKA_AUTO_OFFSET_RESET").unwrap_or_else(|_| "latest".to_string());
    let clickhouse_url =
        env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let db_name = env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "guardianpay".to_string());
    let clickhouse_user = env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string());
    let clickhouse_password = env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();
    let bind_addr: SocketAddr = env::var("ANALYTICS_BIND")
        .unwrap_or_else(|_| "0.0.0.0:8083".to_string())
        .parse()
        .context("invalid ANALYTICS_BIND")?;

    let base_clickhouse = ClickHouseClient::default()
        .with_url(clickhouse_url)
        .with_user(clickhouse_user)
        .with_password(clickhouse_password);

    ensure_schema(&base_clickhouse, &db_name).await?;
    let clickhouse = base_clickhouse.with_database(db_name.clone());

    let app_state = Arc::new(AppState {
        clickhouse: clickhouse.clone(),
        db_name,
    });

    let (decision_tx, mut decision_rx) = mpsc::channel::<DecisionTraceV1>(1024);
    let (ring_tx, mut ring_rx) = mpsc::channel::<GraphSignalV1>(1024);

    tokio::spawn(run_kafka_ingest(
        kafka_brokers.clone(),
        kafka_group_id,
        kafka_auto_offset_reset,
        topic_tx_decision.clone(),
        topic_tx_graph.clone(),
        decision_tx,
        ring_tx,
    ));

    let writer_client = clickhouse.clone();
    tokio::spawn(async move {
        while let Some(trace) = decision_rx.recv().await {
            if let Err(err) = insert_decision(&writer_client, trace).await {
                error!("failed writing decision row: {err}");
            }
        }
    });

    let ring_client = clickhouse.clone();
    tokio::spawn(async move {
        while let Some(graph_signal) = ring_rx.recv().await {
            if let Err(err) = insert_ring_signal(&ring_client, graph_signal).await {
                error!("failed writing ring row: {err}");
            }
        }
    });

    let app = Router::new()
        .route("/v1/health", get(health))
        .route("/internal/decisions/:tx_id", get(get_decision))
        .route("/internal/rings/:ring_id", get(get_ring))
        .with_state(app_state)
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    info!("analytics-writer listening on {bind_addr}");
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn ensure_schema(client: &ClickHouseClient, db_name: &str) -> anyhow::Result<()> {
    client
        .query(&format!("CREATE DATABASE IF NOT EXISTS {}", db_name))
        .execute()
        .await?;

    client
        .query(&format!(
            "
            CREATE TABLE IF NOT EXISTS {}.decisions (
                tx_id String,
                decision String,
                final_score Float64,
                policy_version String,
                model_version String,
                replay_hash String,
                latency_ms UInt64,
                reasons_json String,
                label String,
                event_time String,
                inserted_at String
            ) ENGINE = MergeTree
            ORDER BY (event_time, tx_id)
            ",
            db_name
        ))
        .execute()
        .await?;

    client
        .query(&format!(
            "
            CREATE TABLE IF NOT EXISTS {}.ring_signals (
                ring_id String,
                component_size UInt32,
                avg_graph_score Float64,
                shared_device_fanout UInt32,
                updated_at String
            ) ENGINE = MergeTree
            ORDER BY (updated_at, ring_id)
            ",
            db_name
        ))
        .execute()
        .await?;

    Ok(())
}

async fn run_kafka_ingest(
    kafka_brokers: String,
    kafka_group_id: String,
    kafka_auto_offset_reset: String,
    topic_tx_decision: String,
    topic_tx_graph: String,
    decision_tx: mpsc::Sender<DecisionTraceV1>,
    ring_tx: mpsc::Sender<GraphSignalV1>,
) {
    let consumer: StreamConsumer = match ClientConfig::new()
        .set("group.id", &kafka_group_id)
        .set("bootstrap.servers", &kafka_brokers)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", &kafka_auto_offset_reset)
        .create()
    {
        Ok(c) => c,
        Err(err) => {
            error!("failed to create analytics-writer consumer: {err}");
            return;
        }
    };

    if let Err(err) = consumer.subscribe(&[&topic_tx_decision, &topic_tx_graph]) {
        error!("failed to subscribe analytics-writer consumer: {err}");
        return;
    }

    let mut stream = consumer.stream();
    info!("analytics-writer consuming tx.decision and tx.graph");
    while let Some(result) = stream.next().await {
        match result {
            Ok(msg) => {
                let topic = msg.topic().to_string();
                let Some(payload) = msg.payload() else {
                    continue;
                };
                if topic == topic_tx_decision {
                    match serde_json::from_slice::<DecisionTraceV1>(payload) {
                        Ok(trace) => {
                            if decision_tx.send(trace).await.is_err() {
                                warn!("decision writer channel closed");
                                break;
                            }
                        }
                        Err(err) => warn!("invalid tx.decision payload: {err}"),
                    }
                } else if topic == topic_tx_graph {
                    match serde_json::from_slice::<GraphSignalV1>(payload) {
                        Ok(graph) => {
                            if ring_tx.send(graph).await.is_err() {
                                warn!("ring writer channel closed");
                                break;
                            }
                        }
                        Err(err) => warn!("invalid tx.graph payload: {err}"),
                    }
                }
            }
            Err(err) => error!("analytics-writer consume error: {err}"),
        }
    }
}

async fn insert_decision(client: &ClickHouseClient, trace: DecisionTraceV1) -> anyhow::Result<()> {
    let row = DecisionRow {
        tx_id: trace.tx_id,
        decision: format!("{:?}", trace.decision).to_uppercase(),
        final_score: trace.final_score,
        policy_version: trace.policy_version,
        model_version: trace.model_version,
        replay_hash: trace.replay_hash,
        latency_ms: trace.latency_ms as u64,
        reasons_json: serde_json::to_string(&trace.reasons)?,
        label: trace.label.unwrap_or_else(|| "unknown".to_string()),
        event_time: trace.event_time.to_rfc3339(),
        inserted_at: Utc::now().to_rfc3339(),
    };
    let mut insert = client.insert("decisions")?;
    insert.write(&row).await?;
    insert.end().await?;
    Ok(())
}

async fn insert_ring_signal(client: &ClickHouseClient, graph: GraphSignalV1) -> anyhow::Result<()> {
    let row = RingRow {
        ring_id: graph.ring_id,
        component_size: graph.component_size,
        avg_graph_score: graph.graph_score,
        shared_device_fanout: graph.shared_device_fanout,
        updated_at: Utc::now().to_rfc3339(),
    };
    let mut insert = client.insert("ring_signals")?;
    insert.write(&row).await?;
    insert.end().await?;
    Ok(())
}

async fn health(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok",
        "service": "analytics-writer",
        "db": state.db_name,
        "timestamp": Utc::now()
    }))
}

async fn get_decision(
    State(state): State<Arc<AppState>>,
    Path(tx_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let query = format!(
        "
        SELECT tx_id, decision, final_score, policy_version, model_version, replay_hash, latency_ms, reasons_json, label, event_time, inserted_at
        FROM decisions
        WHERE tx_id = '{}'
        ORDER BY inserted_at DESC
        LIMIT 1
        ",
        tx_id.replace('\'', "")
    );

    let row = state
        .clickhouse
        .query(&query)
        .fetch_one::<DecisionRow>()
        .await
        .map_err(|err| (StatusCode::NOT_FOUND, format!("decision not found: {err}")))?;

    Ok(Json(json!({
        "tx_id": row.tx_id,
        "decision": row.decision,
        "final_score": row.final_score,
        "policy_version": row.policy_version,
        "model_version": row.model_version,
        "replay_hash": row.replay_hash,
        "latency_ms": row.latency_ms,
        "reasons": serde_json::from_str::<serde_json::Value>(&row.reasons_json).unwrap_or_else(|_| json!([])),
        "label": row.label,
        "event_time": row.event_time,
        "inserted_at": row.inserted_at
    })))
}

async fn get_ring(
    State(state): State<Arc<AppState>>,
    Path(ring_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let query = format!(
        "
        SELECT ring_id, component_size, avg_graph_score, shared_device_fanout, updated_at
        FROM ring_signals
        WHERE ring_id = '{}'
        ORDER BY updated_at DESC
        LIMIT 1
        ",
        ring_id.replace('\'', "")
    );

    let row = state
        .clickhouse
        .query(&query)
        .fetch_one::<RingRow>()
        .await
        .map_err(|err| (StatusCode::NOT_FOUND, format!("ring not found: {err}")))?;

    Ok(Json(json!({
        "ring_id": row.ring_id,
        "component_size": row.component_size,
        "avg_graph_score": row.avg_graph_score,
        "shared_device_fanout": row.shared_device_fanout,
        "updated_at": row.updated_at
    })))
}
