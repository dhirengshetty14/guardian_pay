type DecisionResponse = {
  tx_id: string;
  decision: string;
  final_score: number;
  reasons: Array<{ code: string; contribution: number }>;
};

const GATEWAY_URL = "http://localhost:8080";

export default defineContentScript({
  matches: ["http://localhost/*", "http://127.0.0.1/*"],
  main() {
    const panel = document.createElement("div");
    panel.id = "guardianpay-overlay";
    panel.innerHTML = `
      <div class="gp-title">GuardianPay Risk</div>
      <div class="gp-status">Idle</div>
      <button class="gp-btn">Score Checkout</button>
      <div class="gp-detail"></div>
    `;
    const style = document.createElement("style");
    style.textContent = `
      #guardianpay-overlay {
        position: fixed;
        right: 20px;
        bottom: 20px;
        width: 280px;
        z-index: 999999;
        font-family: "Space Grotesk", system-ui, sans-serif;
        background: linear-gradient(130deg, #102341, #0b151f);
        color: #f1f6ff;
        border: 1px solid #385680;
        border-radius: 14px;
        box-shadow: 0 12px 40px rgba(0, 0, 0, 0.4);
        padding: 12px;
      }
      #guardianpay-overlay .gp-title {
        font-weight: 800;
        letter-spacing: 0.02em;
      }
      #guardianpay-overlay .gp-status {
        margin-top: 6px;
        font-size: 0.9rem;
        opacity: 0.9;
      }
      #guardianpay-overlay .gp-btn {
        margin-top: 8px;
        width: 100%;
        border: 0;
        border-radius: 10px;
        padding: 8px 10px;
        font-weight: 700;
        background: linear-gradient(120deg, #2f7ef6, #0fc3a2);
        color: #fff;
        cursor: pointer;
      }
      #guardianpay-overlay .gp-detail {
        margin-top: 8px;
        font-size: 0.82rem;
        line-height: 1.35;
      }
    `;
    document.documentElement.appendChild(style);
    document.body.appendChild(panel);

    const statusEl = panel.querySelector(".gp-status") as HTMLDivElement;
    const detailEl = panel.querySelector(".gp-detail") as HTMLDivElement;
    const button = panel.querySelector(".gp-btn") as HTMLButtonElement;

    button.addEventListener("click", async () => {
      button.disabled = true;
      statusEl.textContent = "Submitting transaction...";
      detailEl.textContent = "";
      try {
        const payload = buildPayload();
        const queued = await queueTransaction(payload);
        statusEl.textContent = `Queued tx: ${queued.tx_id.slice(0, 8)}...`;
        const result = await pollDecision(queued.tx_id, 7, 900);
        if (!result) {
          statusEl.textContent = "No decision yet";
          detailEl.textContent = "Try again after stream processors catch up.";
        } else {
          statusEl.textContent = `${result.decision} (${result.final_score.toFixed(3)})`;
          detailEl.innerHTML = result.reasons
            .slice(0, 3)
            .map((r) => `<div>${r.code}: ${r.contribution.toFixed(3)}</div>`)
            .join("");
        }
      } catch (err) {
        statusEl.textContent = "Failed";
        detailEl.textContent = String(err);
      } finally {
        button.disabled = false;
      }
    });
  },
});

function buildPayload(): Record<string, unknown> {
  const amountInput =
    document.querySelector<HTMLInputElement>('input[name="amount"]') ??
    document.querySelector<HTMLInputElement>('input[type="number"]');
  const amount = Number(amountInput?.value ?? "79.99");

  return {
    account_id: getOrCreateFingerprint("gp_account", "acct_ext"),
    card_id: getOrCreateFingerprint("gp_card", "card_ext"),
    merchant_id: location.hostname.replaceAll(".", "_"),
    amount: Number.isFinite(amount) ? Math.max(amount, 1) : 79.99,
    currency: "USD",
    country: "US",
    device_id: getOrCreateFingerprint("gp_device", "dev_ext"),
    ip: "10.23.45.67",
    mcc: "5999",
    channel: "web",
    is_card_present: false,
    user_agent: navigator.userAgent.slice(0, 160),
  };
}

function getOrCreateFingerprint(key: string, prefix: string): string {
  const cached = localStorage.getItem(key);
  if (cached) return cached;
  const created = `${prefix}_${Math.floor(Math.random() * 1000000)}`;
  localStorage.setItem(key, created);
  return created;
}

async function queueTransaction(payload: Record<string, unknown>): Promise<{ tx_id: string }> {
  const response = await fetch(`${GATEWAY_URL}/v1/transactions/score-preview`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    throw new Error(await response.text());
  }
  return (await response.json()) as { tx_id: string };
}

async function pollDecision(
  txId: string,
  tries: number,
  delayMs: number
): Promise<DecisionResponse | null> {
  for (let i = 0; i < tries; i++) {
    const response = await fetch(`${GATEWAY_URL}/v1/transactions/${txId}/decision`);
    if (response.ok) {
      return (await response.json()) as DecisionResponse;
    }
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }
  return null;
}
