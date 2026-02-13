use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionEventV1 {
    pub tx_id: String,
    pub event_time: DateTime<Utc>,
    pub account_id: String,
    pub card_id: String,
    pub merchant_id: String,
    pub amount: f64,
    pub currency: String,
    pub country: String,
    pub device_id: String,
    pub ip: String,
    pub mcc: String,
    pub channel: String,
    pub is_card_present: bool,
    pub user_agent: String,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureSignalV1 {
    pub tx_id: String,
    pub velocity_1m_card: f64,
    pub velocity_5m_device: f64,
    pub velocity_15m_ip: f64,
    pub amount_zscore_1h_account: f64,
    pub anomaly_score: f64,
    pub feature_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSignalV1 {
    pub tx_id: String,
    pub ring_id: String,
    pub component_size: u32,
    pub shared_device_fanout: u32,
    pub node_centrality: f64,
    pub graph_score: f64,
    pub graph_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReasonV1 {
    pub code: String,
    pub source: String,
    pub value: f64,
    pub threshold: f64,
    pub contribution: f64,
    pub evidence_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionTraceV1 {
    pub tx_id: String,
    pub decision: Decision,
    pub final_score: f64,
    pub reasons: Vec<ReasonV1>,
    pub policy_version: String,
    pub model_version: String,
    pub replay_hash: String,
    pub latency_ms: u128,
    pub event_time: DateTime<Utc>,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Decision {
    Approve,
    Review,
    Decline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionRequest {
    pub transaction: TransactionEventV1,
    pub feature: FeatureSignalV1,
    pub graph: GraphSignalV1,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RingSummaryV1 {
    pub ring_id: String,
    pub component_size: u32,
    pub avg_graph_score: f64,
    pub shared_device_fanout: u32,
    pub updated_at: DateTime<Utc>,
}

pub fn compute_replay_hash(
    transaction: &TransactionEventV1,
    feature: &FeatureSignalV1,
    graph: &GraphSignalV1,
    decision: &Decision,
    policy_version: &str,
    model_version: &str,
) -> String {
    let normalized = json!({
      "transaction": transaction,
      "feature": feature,
      "graph": graph,
      "decision": decision,
      "policy_version": policy_version,
      "model_version": model_version
    });
    let mut hasher = Sha256::new();
    hasher.update(
        serde_json::to_vec(&normalized).expect("failed to serialize replay hash payload"),
    );
    hex::encode(hasher.finalize())
}
