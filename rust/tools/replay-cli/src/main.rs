use std::collections::HashMap;
use std::env;
use std::time::Duration;

use anyhow::Context;
use common_models::{
    compute_replay_hash, Decision, DecisionTraceV1, FeatureSignalV1, GraphSignalV1,
    TransactionEventV1,
};
use futures::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{config::ClientConfig, Message};
use serde::Serialize;
use tracing::{error, info, warn};

#[derive(Default)]
struct ReplayState {
    tx: HashMap<String, TransactionEventV1>,
    feature: HashMap<String, FeatureSignalV1>,
    graph: HashMap<String, GraphSignalV1>,
    decision: HashMap<String, DecisionTraceV1>,
}

#[derive(Debug, Serialize)]
struct ReplayReport {
    total_checked: usize,
    matches: usize,
    mismatches: usize,
    mismatch_rate: f64,
    mismatched_tx_ids: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();

    let kafka_brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let topic_tx_raw = env::var("TOPIC_TX_RAW").unwrap_or_else(|_| "tx.raw".to_string());
    let topic_tx_feature = env::var("TOPIC_TX_FEATURE").unwrap_or_else(|_| "tx.feature".to_string());
    let topic_tx_graph = env::var("TOPIC_TX_GRAPH").unwrap_or_else(|_| "tx.graph".to_string());
    let topic_tx_decision =
        env::var("TOPIC_TX_DECISION").unwrap_or_else(|_| "tx.decision".to_string());
    let max_messages: usize = env::var("REPLAY_MAX_MESSAGES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let model_version = env::var("MODEL_VERSION").unwrap_or_else(|_| "model-v1".to_string());

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "replay-cli-v1")
        .set("bootstrap.servers", &kafka_brokers)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .context("failed to create replay consumer")?;
    consumer
        .subscribe(&[&topic_tx_raw, &topic_tx_feature, &topic_tx_graph, &topic_tx_decision])
        .context("failed to subscribe replay topics")?;

    let mut stream = consumer.stream();
    let mut state = ReplayState::default();
    let mut seen = 0usize;
    info!("replay-cli scanning up to {max_messages} messages");

    while seen < max_messages {
        match tokio::time::timeout(Duration::from_millis(700), stream.next()).await {
            Ok(Some(Ok(msg))) => {
                seen += 1;
                let topic = msg.topic().to_string();
                let Some(payload) = msg.payload() else {
                    continue;
                };
                match topic.as_str() {
                    t if t == topic_tx_raw => {
                        if let Ok(tx) = serde_json::from_slice::<TransactionEventV1>(payload) {
                            state.tx.insert(tx.tx_id.clone(), tx);
                        }
                    }
                    t if t == topic_tx_feature => {
                        if let Ok(feature) = serde_json::from_slice::<FeatureSignalV1>(payload) {
                            state.feature.insert(feature.tx_id.clone(), feature);
                        }
                    }
                    t if t == topic_tx_graph => {
                        if let Ok(graph) = serde_json::from_slice::<GraphSignalV1>(payload) {
                            state.graph.insert(graph.tx_id.clone(), graph);
                        }
                    }
                    t if t == topic_tx_decision => {
                        if let Ok(decision) = serde_json::from_slice::<DecisionTraceV1>(payload) {
                            state.decision.insert(decision.tx_id.clone(), decision);
                        }
                    }
                    _ => {}
                }
            }
            Ok(Some(Err(err))) => {
                error!("replay consume error: {err}");
            }
            Ok(None) => break,
            Err(_) => {
                info!("replay scan timeout reached with no new records");
                break;
            }
        }
    }

    let report = build_report(state, &model_version);
    println!("{}", serde_json::to_string_pretty(&report)?);
    if report.mismatch_rate > 0.5 {
        warn!("high replay mismatch rate detected");
    }
    Ok(())
}

fn build_report(state: ReplayState, model_version: &str) -> ReplayReport {
    let mut matches = 0usize;
    let mut mismatches = vec![];

    for (tx_id, decision_trace) in &state.decision {
        let Some(tx) = state.tx.get(tx_id) else {
            continue;
        };
        let Some(feature) = state.feature.get(tx_id) else {
            continue;
        };
        let Some(graph) = state.graph.get(tx_id) else {
            continue;
        };

        let policy_version = decision_trace.policy_version.clone();
        let decision = parse_decision_from_text(&format!("{:?}", decision_trace.decision))
            .unwrap_or(Decision::Review);
        let recomputed = compute_replay_hash(
            tx,
            feature,
            graph,
            &decision,
            &policy_version,
            model_version,
        );
        if recomputed == decision_trace.replay_hash {
            matches += 1;
        } else {
            mismatches.push(tx_id.clone());
        }
    }

    let total_checked = matches + mismatches.len();
    let mismatch_rate = if total_checked > 0 {
        mismatches.len() as f64 / total_checked as f64
    } else {
        0.0
    };

    ReplayReport {
        total_checked,
        matches,
        mismatches: mismatches.len(),
        mismatch_rate,
        mismatched_tx_ids: mismatches.into_iter().take(100).collect(),
    }
}

fn parse_decision_from_text(value: &str) -> Option<Decision> {
    match value.to_uppercase().as_str() {
        "APPROVE" => Some(Decision::Approve),
        "REVIEW" => Some(Decision::Review),
        "DECLINE" => Some(Decision::Decline),
        _ => None,
    }
}
