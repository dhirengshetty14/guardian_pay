# GuardianPay X

GuardianPay X is a local-first, distributed fraud detection mesh designed for interview demonstrations.
It combines streaming features, graph ring signals, policy decisions, and live analytics.

## Stack

- Rust services (`gateway-api`, `graph-service`, `risk-decider`, `analytics-writer`, `replay-cli`)
- Python streaming jobs (`simulator`, `feature-stream`)
- Kafka + Neo4j + Redis + ClickHouse + OPA
- Svelte dashboard + MV3 browser extension
- Docker Compose profiles (`core`, `full`)

## Architecture

1. `simulator` and `gateway-api` publish transaction events to `tx.raw`.
2. `feature-stream` computes velocity/anomaly features and emits `tx.feature`.
3. `graph-service` updates graph context and emits `tx.graph`.
4. `risk-decider` joins `tx.raw` + `tx.feature` + `tx.graph`, calls OPA, emits `tx.decision`.
5. `analytics-writer` stores decisions/ring summaries in ClickHouse.
6. `gateway-api` exposes REST + WebSocket APIs for dashboard/extension clients.
7. `replay-cli` validates deterministic decision hash consistency.

## Repository Layout

```text
guardian_pay/
  docker-compose.yml
  infra/
    kafka/
    opa/
    observability/
  rust/
    crates/common-models
    services/{gateway-api,graph-service,risk-decider,analytics-writer}
    tools/replay-cli
  python/
    simulator/
    feature-stream/
    flink-feature-job/
  dashboard/
  extension/
  contracts/
  scripts/
  demo/checkout/
```

## Topics

- `tx.raw` (12)
- `tx.feature` (12)
- `tx.graph` (12)
- `tx.decision` (12)
- `tx.deadletter` (3)
- `tx.replay.report` (1)

## APIs

- `POST /v1/transactions/score-preview`
- `POST /v1/simulations/run`
- `GET /v1/transactions/{tx_id}/decision`
- `GET /v1/rings/{ring_id}`
- `GET /v1/rings/{ring_id}/graph`
- `POST /v1/replay/run`
- `GET /v1/stream/alerts` (WebSocket)
- `GET /v1/health`

## Quick Start

Requirements:

- Docker Desktop with Compose
- Optional local Rust toolchain for non-container builds
- Optional Node.js for local dashboard/extension builds

Start core stack:

```bash
docker compose --profile core up --build
```

Open:

- Dashboard: `http://localhost:5173`
- Gateway API: `http://localhost:8080/v1/health`
- Neo4j Browser: `http://localhost:7474` (`neo4j` / `guardianpay`)
- ClickHouse HTTP: `http://localhost:8123` (`guardianpay` / `guardianpay`)

Trigger scenario:

```bash
curl -X POST http://localhost:8080/v1/simulations/run ^
  -H "Content-Type: application/json" ^
  -d "{\"scenario\":\"ring_expansion\",\"count\":25000,\"rate_per_sec\":600}"
```

Run replay check:

```bash
docker compose --profile tools run --rm replay-cli
```

Run synthetic quality evaluation (precision/recall from labeled decisions):

```powershell
./scripts/run-quality.ps1
```

Run smoke test:

```powershell
./scripts/smoke-test.ps1
```

## Demo Flow

1. Start `core` profile.
2. Open dashboard and run `ring_expansion`.
3. Watch live decision feed update over WebSocket.
4. Query a ring id via ring lookup panel.
5. Run replay check and show mismatch rate.
6. Open checkout demo page and use extension overlay.

## Extension Demo

1. Build extension:
   - `cd extension && npm install && npm run build`
2. Load unpacked extension from `.output/chrome-mv3`.
3. Open `http://localhost:8787` checkout demo page.
4. Click `Score Checkout` in the extension overlay.

## Profiles

- `core`: end-to-end fraud mesh.
- `full`: `core` + Flink + Prometheus + Loki + Tempo + Grafana.

## Current Status

- Core event pipeline and decisions implemented.
- Dashboard and extension implemented.
- Flink profile scaffold included for expanded stream-processing demos.
- Additional hardening (tests, metrics instrumentation, CI) is planned.
