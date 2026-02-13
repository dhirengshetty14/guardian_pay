use std::collections::{HashMap, HashSet};
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use chrono::Utc;
use common_models::{GraphSignalV1, TransactionEventV1};
use futures::StreamExt;
use neo4rs::{query, Graph};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{config::ClientConfig, Message};
use serde::Serialize;
use serde_json::json;
use tokio::sync::Mutex;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{error, info, warn};

#[derive(Default)]
struct GraphState {
    device_accounts: HashMap<String, HashSet<String>>,
    ip_accounts: HashMap<String, HashSet<String>>,
    account_cards: HashMap<String, HashSet<String>>,
    account_merchants: HashMap<String, HashSet<String>>,
    account_seen_count: HashMap<String, u64>,
}

#[derive(Clone)]
struct ApiState {
    graph: Option<Arc<Graph>>,
    local_state: Arc<Mutex<GraphState>>,
}

#[derive(Debug, Clone, Serialize)]
struct RingNode {
    id: String,
    kind: String,
    label: String,
}

#[derive(Debug, Clone, Serialize)]
struct RingEdge {
    id: String,
    source: String,
    target: String,
    relation: String,
}

#[derive(Debug, Clone, Serialize)]
struct RingGraphResponse {
    ring_id: String,
    node_count: usize,
    edge_count: usize,
    nodes: Vec<RingNode>,
    edges: Vec<RingEdge>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();

    let kafka_brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let topic_tx_raw = env::var("TOPIC_TX_RAW").unwrap_or_else(|_| "tx.raw".to_string());
    let topic_tx_graph = env::var("TOPIC_TX_GRAPH").unwrap_or_else(|_| "tx.graph".to_string());
    let kafka_group_id =
        env::var("KAFKA_GROUP_ID").unwrap_or_else(|_| "graph-service-v2".to_string());
    let kafka_auto_offset_reset =
        env::var("KAFKA_AUTO_OFFSET_RESET").unwrap_or_else(|_| "latest".to_string());
    let bind_addr: SocketAddr = env::var("GRAPH_BIND")
        .unwrap_or_else(|_| "0.0.0.0:8084".to_string())
        .parse()
        .context("invalid GRAPH_BIND")?;

    let neo4j_uri = env::var("NEO4J_URI").unwrap_or_else(|_| "neo4j://localhost:7687".to_string());
    let neo4j_user = env::var("NEO4J_USER").unwrap_or_else(|_| "neo4j".to_string());
    let neo4j_password = env::var("NEO4J_PASSWORD").unwrap_or_else(|_| "guardianpay".to_string());

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .context("failed to create graph-service Kafka producer")?;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &kafka_group_id)
        .set("bootstrap.servers", &kafka_brokers)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", &kafka_auto_offset_reset)
        .create()
        .context("failed to create graph-service Kafka consumer")?;
    consumer
        .subscribe(&[&topic_tx_raw])
        .context("failed to subscribe to tx.raw")?;

    let graph = match Graph::new(&neo4j_uri, &neo4j_user, &neo4j_password).await {
        Ok(g) => {
            info!("connected to Neo4j at {neo4j_uri}");
            Some(Arc::new(g))
        }
        Err(err) => {
            warn!("Neo4j connection failed, running without graph persistence: {err}");
            None
        }
    };

    let local_state = Arc::new(Mutex::new(GraphState::default()));
    let kafka_graph = graph.clone();
    let kafka_state = local_state.clone();
    tokio::spawn(async move {
        run_kafka_ingest(consumer, producer, topic_tx_graph, kafka_graph, kafka_state).await;
    });

    let app_state = Arc::new(ApiState { graph, local_state });
    let app = Router::new()
        .route("/v1/health", get(health))
        .route("/internal/rings/:ring_id/graph", get(get_ring_graph))
        .with_state(app_state)
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    info!("graph-service api listening on {bind_addr}");
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn run_kafka_ingest(
    consumer: StreamConsumer,
    producer: FutureProducer,
    topic_tx_graph: String,
    graph: Option<Arc<Graph>>,
    local_state: Arc<Mutex<GraphState>>,
) {
    let mut stream = consumer.stream();
    info!("graph-service consuming tx.raw and emitting tx.graph");

    while let Some(message_result) = stream.next().await {
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
                        let body = match serde_json::to_string(&signal) {
                            Ok(v) => v,
                            Err(err) => {
                                error!("failed serializing graph signal for {}: {err}", tx.tx_id);
                                continue;
                            }
                        };
                        if let Err((err, _)) = producer
                            .send(
                                FutureRecord::to(&topic_tx_graph)
                                    .key(&signal.tx_id)
                                    .payload(&body),
                                Timeout::After(std::time::Duration::from_secs(3)),
                            )
                            .await
                        {
                            error!("failed to publish tx.graph for {}: {err}", tx.tx_id);
                        }
                    }
                    Err(err) => error!("invalid tx.raw payload: {err}"),
                }
            }
            Err(err) => error!("graph-service consume error: {err}"),
        }
    }
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
    lock.account_cards
        .entry(tx.account_id.clone())
        .or_insert_with(HashSet::new)
        .insert(tx.card_id.clone());
    lock.account_merchants
        .entry(tx.account_id.clone())
        .or_insert_with(HashSet::new)
        .insert(tx.merchant_id.clone());

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

async fn health() -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok",
        "service": "graph-service",
        "timestamp": Utc::now()
    }))
}

async fn get_ring_graph(
    State(state): State<Arc<ApiState>>,
    Path(ring_id): Path<String>,
) -> Result<Json<RingGraphResponse>, (StatusCode, String)> {
    let Some((device_id, ip)) = parse_ring_id(&ring_id) else {
        return Err((
            StatusCode::BAD_REQUEST,
            "ring id must match format ring:<device_id>:<ip>".to_string(),
        ));
    };

    if let Some(graph) = &state.graph {
        match fetch_ring_graph_from_neo4j(graph.as_ref(), &ring_id, &device_id, &ip).await {
            Ok(response) if !response.nodes.is_empty() => return Ok(Json(response)),
            Ok(_) => {}
            Err(err) => warn!("neo4j ring graph query failed for {ring_id}: {err}"),
        }
    }

    let response =
        fetch_ring_graph_from_state(&ring_id, &device_id, &ip, state.local_state.clone()).await;
    Ok(Json(response))
}

fn parse_ring_id(ring_id: &str) -> Option<(String, String)> {
    let mut parts = ring_id.splitn(3, ':');
    let prefix = parts.next()?;
    let device_id = parts.next()?;
    let ip = parts.next()?;
    if prefix != "ring" || device_id.is_empty() || ip.is_empty() {
        return None;
    }
    Some((device_id.to_string(), ip.to_string()))
}

fn add_node(nodes: &mut HashMap<String, RingNode>, id: String, kind: &str, label: String) {
    nodes.entry(id.clone()).or_insert(RingNode {
        id,
        kind: kind.to_string(),
        label,
    });
}

fn add_edge(edges: &mut HashSet<(String, String, String)>, source: String, target: String, relation: &str) {
    edges.insert((source, target, relation.to_string()));
}

fn finalize_graph(
    ring_id: &str,
    nodes: HashMap<String, RingNode>,
    edges: HashSet<(String, String, String)>,
) -> RingGraphResponse {
    let mut node_list = nodes.into_values().collect::<Vec<_>>();
    node_list.sort_by(|a, b| a.id.cmp(&b.id));

    let mut edge_list = edges
        .into_iter()
        .map(|(source, target, relation)| RingEdge {
            id: format!("{source}|{target}|{relation}"),
            source,
            target,
            relation,
        })
        .collect::<Vec<_>>();
    edge_list.sort_by(|a, b| a.id.cmp(&b.id));

    RingGraphResponse {
        ring_id: ring_id.to_string(),
        node_count: node_list.len(),
        edge_count: edge_list.len(),
        nodes: node_list,
        edges: edge_list,
    }
}

async fn fetch_ring_graph_from_neo4j(
    graph: &Graph,
    ring_id: &str,
    device_id: &str,
    ip: &str,
) -> anyhow::Result<RingGraphResponse> {
    let mut nodes = HashMap::<String, RingNode>::new();
    let mut edges = HashSet::<(String, String, String)>::new();

    let device_node_id = format!("device:{device_id}");
    let ip_node_id = format!("ip:{ip}");
    add_node(&mut nodes, device_node_id.clone(), "device", device_id.to_string());
    add_node(&mut nodes, ip_node_id.clone(), "ip", ip.to_string());

    let account_query = query(
        r#"
        MATCH (a:Account)
        WHERE EXISTS { (a)-[:USES_DEVICE]->(:Device {id: $device_id}) }
           OR EXISTS { (a)-[:USES_IP]->(:IP {id: $ip}) }
        RETURN DISTINCT
            a.id AS account_id,
            EXISTS { (a)-[:USES_DEVICE]->(:Device {id: $device_id}) } AS uses_device,
            EXISTS { (a)-[:USES_IP]->(:IP {id: $ip}) } AS uses_ip
        LIMIT 250
        "#,
    )
    .param("device_id", device_id.to_string())
    .param("ip", ip.to_string());

    let mut result = graph.execute(account_query).await?;
    let mut account_ids = Vec::new();
    while let Ok(Some(row)) = result.next().await {
        let account_id = match row.get::<String>("account_id") {
            Ok(v) => v,
            Err(_) => continue,
        };
        let account_node_id = format!("account:{account_id}");
        add_node(
            &mut nodes,
            account_node_id.clone(),
            "account",
            account_id.clone(),
        );

        if row.get::<bool>("uses_device").unwrap_or(false) {
            add_edge(
                &mut edges,
                account_node_id.clone(),
                device_node_id.clone(),
                "USES_DEVICE",
            );
        }
        if row.get::<bool>("uses_ip").unwrap_or(false) {
            add_edge(
                &mut edges,
                account_node_id.clone(),
                ip_node_id.clone(),
                "USES_IP",
            );
        }
        account_ids.push(account_id);
    }

    if account_ids.is_empty() {
        return Ok(finalize_graph(ring_id, nodes, edges));
    }

    let detail_query = query(
        r#"
        MATCH (a:Account)
        WHERE a.id IN $account_ids
        OPTIONAL MATCH (a)-[:USES_CARD]->(c:Card)
        OPTIONAL MATCH (a)-[:PAYS_MERCHANT]->(m:Merchant)
        RETURN DISTINCT a.id AS account_id, c.id AS card_id, m.id AS merchant_id
        LIMIT 2500
        "#,
    )
    .param("account_ids", account_ids);

    let mut detail_result = graph.execute(detail_query).await?;
    while let Ok(Some(row)) = detail_result.next().await {
        let account_id = match row.get::<String>("account_id") {
            Ok(v) => v,
            Err(_) => continue,
        };
        let account_node_id = format!("account:{account_id}");

        if let Ok(card_id) = row.get::<String>("card_id") {
            let card_node_id = format!("card:{card_id}");
            add_node(&mut nodes, card_node_id.clone(), "card", card_id);
            add_edge(&mut edges, account_node_id.clone(), card_node_id, "USES_CARD");
        }
        if let Ok(merchant_id) = row.get::<String>("merchant_id") {
            let merchant_node_id = format!("merchant:{merchant_id}");
            add_node(
                &mut nodes,
                merchant_node_id.clone(),
                "merchant",
                merchant_id,
            );
            add_edge(
                &mut edges,
                account_node_id.clone(),
                merchant_node_id,
                "PAYS_MERCHANT",
            );
        }
    }

    Ok(finalize_graph(ring_id, nodes, edges))
}

async fn fetch_ring_graph_from_state(
    ring_id: &str,
    device_id: &str,
    ip: &str,
    state: Arc<Mutex<GraphState>>,
) -> RingGraphResponse {
    let lock = state.lock().await;
    let mut nodes = HashMap::<String, RingNode>::new();
    let mut edges = HashSet::<(String, String, String)>::new();

    let device_node_id = format!("device:{device_id}");
    let ip_node_id = format!("ip:{ip}");
    add_node(&mut nodes, device_node_id.clone(), "device", device_id.to_string());
    add_node(&mut nodes, ip_node_id.clone(), "ip", ip.to_string());

    let mut account_ids = HashSet::<String>::new();
    if let Some(accounts) = lock.device_accounts.get(device_id) {
        account_ids.extend(accounts.iter().cloned());
    }
    if let Some(accounts) = lock.ip_accounts.get(ip) {
        account_ids.extend(accounts.iter().cloned());
    }

    for account_id in account_ids {
        let account_node_id = format!("account:{account_id}");
        add_node(
            &mut nodes,
            account_node_id.clone(),
            "account",
            account_id.clone(),
        );
        if lock
            .device_accounts
            .get(device_id)
            .is_some_and(|set| set.contains(&account_id))
        {
            add_edge(
                &mut edges,
                account_node_id.clone(),
                device_node_id.clone(),
                "USES_DEVICE",
            );
        }
        if lock
            .ip_accounts
            .get(ip)
            .is_some_and(|set| set.contains(&account_id))
        {
            add_edge(
                &mut edges,
                account_node_id.clone(),
                ip_node_id.clone(),
                "USES_IP",
            );
        }
        if let Some(cards) = lock.account_cards.get(&account_id) {
            for card_id in cards {
                let card_node_id = format!("card:{card_id}");
                add_node(&mut nodes, card_node_id.clone(), "card", card_id.to_string());
                add_edge(
                    &mut edges,
                    account_node_id.clone(),
                    card_node_id,
                    "USES_CARD",
                );
            }
        }
        if let Some(merchants) = lock.account_merchants.get(&account_id) {
            for merchant_id in merchants {
                let merchant_node_id = format!("merchant:{merchant_id}");
                add_node(
                    &mut nodes,
                    merchant_node_id.clone(),
                    "merchant",
                    merchant_id.to_string(),
                );
                add_edge(
                    &mut edges,
                    account_node_id.clone(),
                    merchant_node_id,
                    "PAYS_MERCHANT",
                );
            }
        }
    }

    finalize_graph(ring_id, nodes, edges)
}
