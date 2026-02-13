use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::Arc;

use anyhow::Context;
use common_models::{GraphSignalV1, TransactionEventV1};
use neo4rs::{query, Graph};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{config::ClientConfig, Message};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

#[derive(Default)]
struct GraphState {
    device_accounts: HashMap<String, HashSet<String>>,
    ip_accounts: HashMap<String, HashSet<String>>,
    account_seen_count: HashMap<String, u64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();

    let kafka_brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let topic_tx_raw = env::var("TOPIC_TX_RAW").unwrap_or_else(|_| "tx.raw".to_string());
    let topic_tx_graph = env::var("TOPIC_TX_GRAPH").unwrap_or_else(|_| "tx.graph".to_string());

    let neo4j_uri = env::var("NEO4J_URI").unwrap_or_else(|_| "neo4j://localhost:7687".to_string());
    let neo4j_user = env::var("NEO4J_USER").unwrap_or_else(|_| "neo4j".to_string());
    let neo4j_password = env::var("NEO4J_PASSWORD").unwrap_or_else(|_| "guardianpay".to_string());

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .context("failed to create graph-service Kafka producer")?;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "graph-service-v1")
        .set("bootstrap.servers", &kafka_brokers)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()
        .context("failed to create graph-service Kafka consumer")?;
    consumer
        .subscribe(&[&topic_tx_raw])
        .context("failed to subscribe to tx.raw")?;

    let graph = match Graph::new(&neo4j_uri, &neo4j_user, &neo4j_password).await {
        Ok(g) => {
            info!("connected to Neo4j at {neo4j_uri}");
            Some(g)
        }
        Err(err) => {
            warn!("Neo4j connection failed, running without graph persistence: {err}");
            None
        }
    };

    let local_state = Arc::new(Mutex::new(GraphState::default()));
    let mut stream = consumer.stream();
    info!("graph-service consuming tx.raw and emitting tx.graph");

    while let Some(message_result) = futures::StreamExt::next(&mut stream).await {
        match message_result {
            Ok(msg) => {
                let Some(payload) = msg.payload() else {
                    continue;
                };
                match serde_json::from_slice::<TransactionEventV1>(payload) {
                    Ok(tx) => {
                        if let Some(neo) = &graph {
                            if let Err(err) = upsert_graph_edges(neo, &tx).await {
                                warn!("failed Neo4j upsert for {}: {err}", tx.tx_id);
                            }
                        }
                        let signal = build_graph_signal(&tx, local_state.clone()).await;
                        let body = serde_json::to_string(&signal)?;
                        producer
                            .send(
                                FutureRecord::to(&topic_tx_graph)
                                    .key(&signal.tx_id)
                                    .payload(&body),
                                Timeout::After(std::time::Duration::from_secs(3)),
                            )
                            .await
                            .map_err(|(err, _)| err)
                            .context("failed to publish tx.graph")?;
                    }
                    Err(err) => error!("invalid tx.raw payload: {err}"),
                }
            }
            Err(err) => error!("graph-service consume error: {err}"),
        }
    }
    Ok(())
}

async fn upsert_graph_edges(graph: &Graph, tx: &TransactionEventV1) -> anyhow::Result<()> {
    let q = query(
        r#"
        MERGE (a:Account {id: $account_id})
        MERGE (c:Card {id: $card_id})
        MERGE (d:Device {id: $device_id})
        MERGE (i:IP {id: $ip})
        MERGE (m:Merchant {id: $merchant_id})
        MERGE (a)-[:USES_CARD]->(c)
        MERGE (a)-[:USES_DEVICE]->(d)
        MERGE (a)-[:USES_IP]->(i)
        MERGE (a)-[:PAYS_MERCHANT]->(m)
        "#,
    )
    .param("account_id", tx.account_id.clone())
    .param("card_id", tx.card_id.clone())
    .param("device_id", tx.device_id.clone())
    .param("ip", tx.ip.clone())
    .param("merchant_id", tx.merchant_id.clone());
    graph.run(q).await?;
    Ok(())
}

async fn build_graph_signal(tx: &TransactionEventV1, state: Arc<Mutex<GraphState>>) -> GraphSignalV1 {
    let mut lock = state.lock().await;

    let device_len = {
        let set = lock
            .device_accounts
            .entry(tx.device_id.clone())
            .or_insert_with(HashSet::new);
        set.insert(tx.account_id.clone());
        set.len()
    };

    let ip_len = {
        let set = lock
            .ip_accounts
            .entry(tx.ip.clone())
            .or_insert_with(HashSet::new);
        set.insert(tx.account_id.clone());
        set.len()
    };

    let seen = lock
        .account_seen_count
        .entry(tx.account_id.clone())
        .or_insert(0);
    let is_new_account = *seen == 0;
    *seen += 1;

    let component_size = (device_len + ip_len).min(u32::MAX as usize) as u32;
    let shared_device_fanout = device_len.min(u32::MAX as usize) as u32;
    let density = ((component_size as f64) / 10.0).clamp(0.0, 1.0);
    let centrality = if component_size > 0 {
        (shared_device_fanout as f64) / (component_size as f64)
    } else {
        0.0
    };
    let fanout = ((shared_device_fanout as f64) / 8.0).clamp(0.0, 1.0);
    let new_account_ratio = if is_new_account { 1.0 } else { 0.0 };
    let graph_score =
        0.35 * density + 0.25 * centrality + 0.20 * fanout + 0.20 * new_account_ratio;

    let ring_id = format!("ring:{}:{}", tx.device_id, tx.ip);

    GraphSignalV1 {
        tx_id: tx.tx_id.clone(),
        ring_id,
        component_size,
        shared_device_fanout,
        node_centrality: centrality,
        graph_score: graph_score.clamp(0.0, 1.0),
        graph_version: "graph-v1".to_string(),
    }
}
