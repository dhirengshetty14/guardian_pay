<script lang="ts">
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

  const gatewayUrl = import.meta.env.VITE_GATEWAY_URL ?? "http://localhost:8080";
  const wsUrl =
    import.meta.env.VITE_WS_URL ?? "ws://localhost:8080/v1/stream/alerts";
  const scenarios = [
    "baseline",
    "card_testing",
    "ring_expansion",
    "account_takeover",
    "bust_out",
  ];

  let selectedScenario = "ring_expansion";
  let count = 10000;
  let ratePerSec = 500;
  let runStatus = "idle";
  let alerts: DecisionAlert[] = [];
  let connectionStatus = "connecting";
  let ringId = "";
  let ringData: Record<string, unknown> | null = null;

  $: total = alerts.length;
  $: declined = alerts.filter((a) => a.decision === "DECLINE").length;
  $: review = alerts.filter((a) => a.decision === "REVIEW").length;
  $: precision =
    alerts.filter((a) => a.decision !== "APPROVE").length === 0
      ? 0
      : alerts.filter((a) => a.decision !== "APPROVE" && a.label === "fraud").length /
        alerts.filter((a) => a.decision !== "APPROVE").length;

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

  async function runScenario(): Promise<void> {
    runStatus = "running";
    ringData = null;
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

  async function fetchRing(): Promise<void> {
    if (!ringId.trim()) return;
    try {
      const response = await fetch(`${gatewayUrl}/v1/rings/${encodeURIComponent(ringId)}`);
      if (!response.ok) {
        throw new Error(await response.text());
      }
      ringData = await response.json();
    } catch (err) {
      ringData = { error: String(err) };
    }
  }

  function format(value: number): string {
    return Number.isFinite(value) ? value.toFixed(3) : "0.000";
  }
</script>

<main>
  <section class="hero">
    <h1>GuardianPay X</h1>
    <p>Real-time fraud risk mesh with graph ring detection and deterministic replay signals.</p>
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
  </section>

  <section class="ring">
    <h2>Ring Lookup</h2>
    <div class="grid">
      <input placeholder="ring:dev_ring_001:192.168.0.2" bind:value={ringId} />
      <button on:click={fetchRing}>Fetch Ring</button>
    </div>
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
    max-width: 1200px;
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
  .table {
    display: grid;
    gap: 6px;
    max-height: 420px;
    overflow: auto;
  }
  .head,
  .row {
    display: grid;
    grid-template-columns: 100px 100px 90px 90px 1fr;
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
  }
  .muted {
    opacity: 0.7;
  }
</style>
