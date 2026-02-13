<script lang="ts">
  import { onDestroy } from "svelte";
  import cytoscape, { type Core } from "cytoscape";

  type Reason = {
    code: string;
    source: string;
    value: number;
    threshold: number;
    contribution: number;
    evidence_ref: string;
  };

  type DecisionAlert = {
    tx_id: string;
    decision: "APPROVE" | "REVIEW" | "DECLINE";
    final_score: number;
    reasons: Reason[];
    policy_version: string;
    model_version: string;
    replay_hash: string;
    latency_ms: number;
    event_time: string;
    label?: string;
  };

  type RingGraphNode = {
    id: string;
    kind: string;
    label: string;
  };

  type RingGraphEdge = {
    id: string;
    source: string;
    target: string;
    relation: string;
  };

  type RingGraphPayload = {
    ring_id: string;
    node_count: number;
    edge_count: number;
    nodes: RingGraphNode[];
    edges: RingGraphEdge[];
  };

  const gatewayUrl = import.meta.env.VITE_GATEWAY_URL ?? "http://localhost:8080";
  const wsUrl = import.meta.env.VITE_WS_URL ?? "ws://localhost:8080/v1/stream/alerts";
  const scenarios = ["baseline", "card_testing", "ring_expansion", "account_takeover", "bust_out"];

  let selectedScenario = "ring_expansion";
  let count = 10000;
  let ratePerSec = 500;
  let runStatus = "idle";
  let alerts: DecisionAlert[] = [];
  let connectionStatus = "connecting";
  let ringId = "";
  let ringData: Record<string, unknown> | null = null;
  let ringGraph: RingGraphPayload | null = null;
  let ringStatus = "idle";

  let ringCanvas: HTMLDivElement | null = null;
  let cy: Core | null = null;

  $: total = alerts.length;
  $: declined = alerts.filter((a) => a.decision === "DECLINE").length;
  $: review = alerts.filter((a) => a.decision === "REVIEW").length;
  $: precision =
    alerts.filter((a) => a.decision !== "APPROVE").length === 0
      ? 0
      : alerts.filter((a) => a.decision !== "APPROVE" && a.label === "fraud").length /
        alerts.filter((a) => a.decision !== "APPROVE").length;
  $: p95Latency = percentile(
    alerts.map((a) => a.latency_ms),
    95
  );

  $: if (ringGraph && ringCanvas) {
    renderRingGraph(ringGraph);
  }

  function percentile(values: number[], p: number): number {
    if (!values.length) return 0;
    const sorted = [...values].sort((a, b) => a - b);
    const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
    return sorted[idx];
  }

  function connectSocket(): void {
    const ws = new WebSocket(wsUrl);
    ws.onopen = () => {
      connectionStatus = "connected";
    };
    ws.onclose = () => {
      connectionStatus = "disconnected";
      setTimeout(connectSocket, 1500);
    };
    ws.onerror = () => {
      connectionStatus = "error";
    };
    ws.onmessage = (event) => {
      try {
        const parsed = JSON.parse(event.data) as DecisionAlert;
        alerts = [parsed, ...alerts].slice(0, 400);
      } catch (err) {
        console.error("invalid ws payload", err);
      }
    };
  }

  connectSocket();

  onDestroy(() => {
    if (cy) {
      cy.destroy();
      cy = null;
    }
  });

  async function runScenario(): Promise<void> {
    runStatus = "running";
    ringData = null;
    ringGraph = null;
    try {
      const response = await fetch(`${gatewayUrl}/v1/simulations/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          scenario: selectedScenario,
          count,
          rate_per_sec: ratePerSec,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      runStatus = "submitted";
    } catch (err) {
      runStatus = `error: ${String(err)}`;
    }
  }

  function extractRingId(alert: DecisionAlert): string | null {
    const graphReason = alert.reasons.find((r) => r.code === "GRAPH_RING_SCORE");
    if (!graphReason || !graphReason.evidence_ref.startsWith("ring:")) {
      return null;
    }
    return graphReason.evidence_ref.slice("ring:".length);
  }

  async function openRingFromAlert(alert: DecisionAlert): Promise<void> {
    const extracted = extractRingId(alert);
    if (!extracted) {
      return;
    }
    ringId = extracted;
    await fetchRing();
  }

  async function fetchRing(): Promise<void> {
    if (!ringId.trim()) return;
    ringStatus = "loading";
    ringData = null;
    ringGraph = null;
    try {
      const [summaryResponse, graphResponse] = await Promise.all([
        fetch(`${gatewayUrl}/v1/rings/${encodeURIComponent(ringId)}`),
        fetch(`${gatewayUrl}/v1/rings/${encodeURIComponent(ringId)}/graph`),
      ]);

      if (!summaryResponse.ok) {
        throw new Error(await summaryResponse.text());
      }
      if (!graphResponse.ok) {
        throw new Error(await graphResponse.text());
      }

      ringData = await summaryResponse.json();
      ringGraph = (await graphResponse.json()) as RingGraphPayload;
      ringStatus = "loaded";
    } catch (err) {
      ringStatus = `error: ${String(err)}`;
      ringData = { error: String(err) };
    }
  }

  function renderRingGraph(payload: RingGraphPayload): void {
    if (!ringCanvas) return;
    const elements = [
      ...payload.nodes.map((node) => ({
        data: {
          id: node.id,
          label: node.label,
          kind: node.kind,
        },
      })),
      ...payload.edges.map((edge) => ({
        data: {
          id: edge.id,
          source: edge.source,
          target: edge.target,
          relation: edge.relation,
        },
      })),
    ];

    if (!cy) {
      cy = cytoscape({
        container: ringCanvas,
        elements,
        style: [
          {
            selector: "node",
            style: {
              label: "data(label)",
              color: "#f4f7ff",
              "font-size": 9,
              "text-outline-width": 2,
              "text-outline-color": "#091325",
            },
          },
          {
            selector: 'node[kind = "account"]',
            style: { "background-color": "#2f7ef6", width: 22, height: 22 },
          },
          {
            selector: 'node[kind = "device"]',
            style: { "background-color": "#19c3aa", width: 26, height: 26 },
          },
          {
            selector: 'node[kind = "ip"]',
            style: { "background-color": "#f3a527", width: 24, height: 24 },
          },
          {
            selector: 'node[kind = "card"]',
            style: { "background-color": "#d25bff", width: 20, height: 20 },
          },
          {
            selector: 'node[kind = "merchant"]',
            style: { "background-color": "#f25f5c", width: 20, height: 20 },
          },
          {
            selector: "edge",
            style: {
              width: 1.5,
              "line-color": "#4a638f",
              "target-arrow-color": "#4a638f",
              "target-arrow-shape": "triangle",
              "curve-style": "bezier",
              label: "data(relation)",
              "font-size": 7,
              color: "#9eb8e7",
              "text-background-opacity": 1,
              "text-background-color": "#07111f",
              "text-background-padding": 2,
            },
          },
        ],
      });
    } else {
      cy.elements().remove();
      cy.add(elements);
    }

    cy.layout({ name: "cose", animate: false, fit: true, padding: 24 }).run();
  }

  function format(value: number): string {
    return Number.isFinite(value) ? value.toFixed(3) : "0.000";
  }
</script>

<main>
  <section class="hero">
    <h1>GuardianPay X</h1>
    <p>Real-time fraud risk mesh with stateful features, graph ring scoring, and replay trace hashes.</p>
    <span class="badge">WS: {connectionStatus}</span>
  </section>

  <section class="controls">
    <h2>Scenario Runner</h2>
    <div class="grid">
      <label>
        Scenario
        <select bind:value={selectedScenario}>
          {#each scenarios as item}
            <option value={item}>{item}</option>
          {/each}
        </select>
      </label>
      <label>
        Count
        <input type="number" bind:value={count} min="1" />
      </label>
      <label>
        Rate/sec
        <input type="number" bind:value={ratePerSec} min="1" />
      </label>
      <button on:click={runScenario}>Run Scenario</button>
    </div>
    <p class="muted">Status: {runStatus}</p>
  </section>

  <section class="metrics">
    <article>
      <h3>Total Alerts</h3>
      <p>{total}</p>
    </article>
    <article>
      <h3>Declined</h3>
      <p>{declined}</p>
    </article>
    <article>
      <h3>Review</h3>
      <p>{review}</p>
    </article>
    <article>
      <h3>Precision (flagged)</h3>
      <p>{(precision * 100).toFixed(1)}%</p>
    </article>
    <article>
      <h3>p95 Latency</h3>
      <p>{p95Latency}ms</p>
    </article>
  </section>

  <section class="ring">
    <h2>Ring Graph Explorer</h2>
    <div class="grid">
      <input placeholder="ring:dev_ring_001:192.168.0.2" bind:value={ringId} />
      <button on:click={fetchRing}>Fetch Ring</button>
    </div>
    <p class="muted">Status: {ringStatus}</p>
    {#if ringGraph}
      <div class="ring-metrics">
        <span>Nodes: {ringGraph.node_count}</span>
        <span>Edges: {ringGraph.edge_count}</span>
      </div>
      <div class="ring-graph" bind:this={ringCanvas}></div>
    {/if}
    {#if ringData}
      <pre>{JSON.stringify(ringData, null, 2)}</pre>
    {/if}
  </section>

  <section class="alerts">
    <h2>Live Decision Feed</h2>
    <div class="table">
      <div class="head">
        <span>Time</span>
        <span>Decision</span>
        <span>Score</span>
        <span>Latency</span>
        <span>Reasons</span>
        <span>Actions</span>
      </div>
      {#each alerts as alert}
        <div class="row">
          <span>{new Date(alert.event_time).toLocaleTimeString()}</span>
          <span class={`decision ${alert.decision.toLowerCase()}`}>{alert.decision}</span>
          <span>{format(alert.final_score)}</span>
          <span>{alert.latency_ms}ms</span>
          <span class="reasons">
            {#each alert.reasons.slice(0, 3) as r}
              <small>{r.code} ({format(r.contribution)})</small>
            {/each}
          </span>
          <span>
            <button class="mini" on:click={() => openRingFromAlert(alert)}>Open Ring</button>
          </span>
        </div>
      {/each}
    </div>
  </section>
</main>

<style>
  :global(body) {
    margin: 0;
    font-family: "Space Grotesk", "Segoe UI", sans-serif;
    background: radial-gradient(circle at top left, #101a2b, #05070d 52%);
    color: #edf2ff;
  }
  main {
    max-width: 1240px;
    margin: 0 auto;
    padding: 24px;
  }
  .hero h1 {
    margin: 0 0 8px;
    font-size: clamp(2rem, 4vw, 3.2rem);
  }
  .badge {
    display: inline-block;
    margin-top: 8px;
    background: #1e2c42;
    padding: 6px 10px;
    border-radius: 10px;
    font-size: 0.85rem;
  }
  section {
    background: rgba(8, 13, 23, 0.85);
    border: 1px solid rgba(117, 144, 197, 0.3);
    border-radius: 14px;
    padding: 16px;
    margin-top: 16px;
  }
  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 10px;
    align-items: end;
  }
  label {
    display: grid;
    gap: 4px;
    font-size: 0.9rem;
  }
  input,
  select,
  button {
    border-radius: 10px;
    border: 1px solid #435776;
    background: #0f1a2e;
    color: #f4f7ff;
    padding: 10px;
  }
  button {
    cursor: pointer;
    background: linear-gradient(120deg, #2f7ef6, #1ac2a8);
    border: none;
    font-weight: 700;
  }
  .mini {
    padding: 6px 8px;
    font-size: 0.75rem;
  }
  .metrics {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 10px;
  }
  .metrics article {
    background: #0a1325;
    border-radius: 10px;
    padding: 12px;
    border: 1px solid #263c61;
  }
  .metrics p {
    margin: 0;
    font-size: 1.5rem;
    font-weight: 700;
  }
  .ring-metrics {
    margin: 10px 0;
    display: flex;
    gap: 12px;
    font-size: 0.9rem;
    opacity: 0.9;
  }
  .ring-graph {
    width: 100%;
    height: 420px;
    border-radius: 10px;
    border: 1px solid #203252;
    background: #050a14;
  }
  .table {
    display: grid;
    gap: 6px;
    max-height: 440px;
    overflow: auto;
  }
  .head,
  .row {
    display: grid;
    grid-template-columns: 100px 100px 90px 90px 1fr 100px;
    gap: 8px;
    align-items: center;
  }
  .head {
    font-size: 0.8rem;
    opacity: 0.8;
    text-transform: uppercase;
  }
  .row {
    border: 1px solid #243754;
    border-radius: 8px;
    padding: 8px;
    background: #0b1528;
    font-size: 0.9rem;
  }
  .decision {
    padding: 4px 8px;
    border-radius: 999px;
    width: fit-content;
    font-weight: 700;
  }
  .decision.approve {
    background: #155e4d;
  }
  .decision.review {
    background: #7f5c13;
  }
  .decision.decline {
    background: #7d2137;
  }
  .reasons {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
  }
  .reasons small {
    background: #152238;
    border-radius: 999px;
    padding: 3px 8px;
  }
  pre {
    overflow: auto;
    background: #050a14;
    padding: 12px;
    border-radius: 10px;
    border: 1px solid #203252;
    max-height: 220px;
  }
  .muted {
    opacity: 0.7;
  }
  @media (max-width: 900px) {
    .head,
    .row {
      grid-template-columns: 80px 90px 70px 70px 1fr 86px;
      font-size: 0.8rem;
    }
    .ring-graph {
      height: 340px;
    }
  }
</style>
