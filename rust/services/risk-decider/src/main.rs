use std::collections::HashMap;
use std::env;
use std::time::Instant;

use anyhow::Context;
use chrono::Utc;
use common_models::{
    compute_replay_hash, Decision, DecisionTraceV1, FeatureSignalV1, GraphSignalV1, ReasonV1,
    TransactionEventV1,
};
use futures::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{config::ClientConfig, Message};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

#[derive(Debug, Default)]
struct PendingState {
    tx: HashMap<String, TransactionEventV1>,
    feature: HashMap<String, FeatureSignalV1>,
    graph: HashMap<String, GraphSignalV1>,
}

#[derive(Debug, Deserialize)]
struct OpaEnvelope {
    result: Option<OpaResult>,
}

#[derive(Debug, Deserialize)]
struct OpaResult {
    decision: Option<String>,
    final_score: Option<f64>,
    reasons: Option<Vec<ReasonV1>>,
    policy_version: Option<String>,
}

#[derive(Debug, Serialize)]
struct OpaInput<'a> {
    transaction: &'a TransactionEventV1,
    feature: &'a FeatureSignalV1,
    graph: &'a GraphSignalV1,
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
    let topic_tx_deadletter =
        env::var("TOPIC_TX_DEADLETTER").unwrap_or_else(|_| "tx.deadletter".to_string());
    let kafka_group_id =
        env::var("KAFKA_GROUP_ID").unwrap_or_else(|_| "risk-decider-v2".to_string());
    let kafka_auto_offset_reset =
        env::var("KAFKA_AUTO_OFFSET_RESET").unwrap_or_else(|_| "latest".to_string());
    let opa_url = env::var("OPA_URL")
        .unwrap_or_else(|_| "http://localhost:8181/v1/data/guardianpay/decision".to_string());
    let model_version = env::var("MODEL_VERSION").unwrap_or_else(|_| "model-v1".to_string());

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &kafka_group_id)
        .set("bootstrap.servers", &kafka_brokers)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", &kafka_auto_offset_reset)
        .create()
        .context("failed to create risk-decider consumer")?;
    consumer
        .subscribe(&[&topic_tx_raw, &topic_tx_feature, &topic_tx_graph])
        .context("failed to subscribe topics in risk-decider")?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .context("failed to create risk-decider producer")?;

    let client = reqwest::Client::new();
    let mut pending = PendingState::default();
    let mut stream = consumer.stream();
    info!("risk-decider consuming tx.raw/tx.feature/tx.graph and publishing tx.decision");

    while let Some(message_result) = stream.next().await {
        match message_result {
            Ok(msg) => {
                let topic = msg.topic().to_string();
                let Some(payload) = msg.payload() else {
                    continue;
                };

                match topic.as_str() {
                    t if t == topic_tx_raw => {
                        if let Ok(tx) = serde_json::from_slice::<TransactionEventV1>(payload) {
                            pending.tx.insert(tx.tx_id.clone(), tx);
                        } else {
                            publish_deadletter(
                                &producer,
                                &topic_tx_deadletter,
                                payload,
                                "failed to parse TransactionEventV1",
                            )
                            .await;
                            continue;
                        }
                    }
                    t if t == topic_tx_feature => {
                        if let Ok(feature) = serde_json::from_slice::<FeatureSignalV1>(payload) {
                            pending.feature.insert(feature.tx_id.clone(), feature);
                        } else {
                            publish_deadletter(
                                &producer,
                                &topic_tx_deadletter,
                                payload,
                                "failed to parse FeatureSignalV1",
                            )
                            .await;
                            continue;
                        }
                    }
                    t if t == topic_tx_graph => {
                        if let Ok(graph) = serde_json::from_slice::<GraphSignalV1>(payload) {
                            pending.graph.insert(graph.tx_id.clone(), graph);
                        } else {
                            publish_deadletter(
                                &producer,
                                &topic_tx_deadletter,
                                payload,
                                "failed to parse GraphSignalV1",
                            )
                            .await;
                            continue;
                        }
                    }
                    _ => continue,
                }

                if let Some(tx_id) = extract_tx_id(payload) {
                    if pending.tx.contains_key(&tx_id)
                        && pending.feature.contains_key(&tx_id)
                        && pending.graph.contains_key(&tx_id)
                    {
                        let tx = pending.tx.remove(&tx_id).expect("checked existence");
                        let feature = pending.feature.remove(&tx_id).expect("checked existence");
                        let graph = pending.graph.remove(&tx_id).expect("checked existence");
                        let trace = evaluate_decision(
                            &client,
                            &opa_url,
                            tx,
                            feature,
                            graph,
                            &model_version,
                        )
                        .await;
                        let body = serde_json::to_string(&trace)?;
                        producer
                            .send(
                                FutureRecord::to(&topic_tx_decision)
                                    .key(&trace.tx_id)
                                    .payload(&body),
                                Timeout::After(std::time::Duration::from_secs(3)),
                            )
                            .await
                            .map_err(|(err, _)| err)
                            .context("failed to publish tx.decision")?;
                    }
                }
            }
            Err(err) => error!("risk-decider consume error: {err}"),
        }
    }

    Ok(())
}

async fn evaluate_decision(
    client: &reqwest::Client,
    opa_url: &str,
    tx: TransactionEventV1,
    feature: FeatureSignalV1,
    graph: GraphSignalV1,
    model_version: &str,
) -> DecisionTraceV1 {
    let start = Instant::now();
    let mut policy_version = "policy-v1".to_string();
    let mut reasons: Vec<ReasonV1> = vec![];
    let mut final_score = (feature.anomaly_score * 0.6 + graph.graph_score * 0.4).clamp(0.0, 1.0);
    let mut decision = baseline_decision(final_score);

    let request_body = serde_json::json!({ "input": OpaInput {
        transaction: &tx,
        feature: &feature,
        graph: &graph,
    }});

    match client.post(opa_url).json(&request_body).send().await {
        Ok(resp) if resp.status().is_success() => {
            match resp.json::<OpaEnvelope>().await {
                Ok(envelope) => {
                    if let Some(result) = envelope.result {
                        if let Some(score) = result.final_score {
                            final_score = score.clamp(0.0, 1.0);
                        }
                        if let Some(pv) = result.policy_version {
                            policy_version = pv;
                        }
                        if let Some(reasons_from_opa) = result.reasons {
                            reasons = reasons_from_opa;
                        }
                        if let Some(d) = result.decision {
                            decision = parse_decision(&d).unwrap_or_else(|| baseline_decision(final_score));
                        }
                    }
                }
                Err(err) => warn!("OPA response parse failed: {err}"),
            }
        }
        Ok(resp) => warn!("OPA returned non-2xx status: {}", resp.status()),
        Err(err) => warn!("OPA call failed, fallback scoring used: {err}"),
    }

    if reasons.is_empty() {
        reasons = fallback_reasons(&feature, &graph);
    }

    let replay_hash = compute_replay_hash(
        &tx,
        &feature,
        &graph,
        &decision,
        &policy_version,
        model_version,
    );

    DecisionTraceV1 {
        tx_id: tx.tx_id.clone(),
        decision,
        final_score,
        reasons,
        policy_version,
        model_version: model_version.to_string(),
        replay_hash,
        latency_ms: start.elapsed().as_millis(),
        event_time: Utc::now(),
        label: tx.label,
    }
}

fn extract_tx_id(payload: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
    value.get("tx_id")?.as_str().map(ToString::to_string)
}

fn parse_decision(value: &str) -> Option<Decision> {
    match value.to_uppercase().as_str() {
        "APPROVE" => Some(Decision::Approve),
        "REVIEW" => Some(Decision::Review),
        "DECLINE" => Some(Decision::Decline),
        _ => None,
    }
}

fn baseline_decision(score: f64) -> Decision {
    if score >= 0.75 {
        Decision::Decline
    } else if score >= 0.45 {
        Decision::Review
    } else {
        Decision::Approve
    }
}

fn fallback_reasons(feature: &FeatureSignalV1, graph: &GraphSignalV1) -> Vec<ReasonV1> {
    vec![
        ReasonV1 {
            code: "ANOMALY_SCORE".to_string(),
            source: "feature".to_string(),
            value: feature.anomaly_score,
            threshold: 0.5,
            contribution: 0.6 * feature.anomaly_score,
            evidence_ref: format!("feature:{}", feature.feature_version),
        },
        ReasonV1 {
            code: "GRAPH_RING_SCORE".to_string(),
            source: "graph".to_string(),
            value: graph.graph_score,
            threshold: 0.5,
            contribution: 0.4 * graph.graph_score,
            evidence_ref: format!("ring:{}", graph.ring_id),
        },
    ]
}

async fn publish_deadletter(
    producer: &FutureProducer,
    topic: &str,
    payload: &[u8],
    reason: &str,
) {
    let body = serde_json::json!({
        "reason": reason,
        "payload": String::from_utf8_lossy(payload),
        "ts": Utc::now()
    })
    .to_string();
    let _ = producer
        .send(
            FutureRecord::to(topic).payload(&body).key("deadletter"),
            Timeout::After(std::time::Duration::from_secs(2)),
        )
        .await;
}
