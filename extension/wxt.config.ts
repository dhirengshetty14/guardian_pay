import { defineConfig } from "wxt";

export default defineConfig({
  manifest: {
    name: "GuardianPay Risk Overlay",
    description: "Scores fake checkout pages and displays fraud risk in real time.",
    permissions: ["storage", "activeTab"],
    host_permissions: [
      "http://localhost/*",
      "http://127.0.0.1/*",
      "http://localhost:8080/*",
      "http://127.0.0.1:8080/*"
    ]
  }
});
