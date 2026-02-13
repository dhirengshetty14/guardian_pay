package guardianpay
import rego.v1

default decision := {
  "decision": "APPROVE",
  "final_score": 0.0,
  "reasons": [],
  "policy_version": "policy-v1"
}

decision := {
  "decision": verdict,
  "final_score": final_score,
  "reasons": reasons,
  "policy_version": "policy-v1"
} if {
  tx := input.transaction
  feature := input.feature
  graph := input.graph

  velocity_component := clamp((0.45 * feature.velocity_1m_card + 0.35 * feature.velocity_5m_device + 0.20 * feature.velocity_15m_ip) / 20.0)
  anomaly_component := clamp(feature.anomaly_score)
  graph_component := clamp(graph.graph_score)
  amount_component := clamp(abs(feature.amount_zscore_1h_account) / 6.0)

  final_score := clamp(0.35 * velocity_component + 0.30 * anomaly_component + 0.25 * graph_component + 0.10 * amount_component)
  verdict := determine_verdict(final_score)
  reasons := build_reasons(feature, graph, velocity_component, anomaly_component)
  tx.tx_id != ""
}

determine_verdict(score) := "DECLINE" if score >= 0.75
determine_verdict(score) := "REVIEW" if {
  score >= 0.45
  score < 0.75
}
determine_verdict(score) := "APPROVE" if score < 0.45

build_reasons(feature, graph, velocity_component, anomaly_component) := reasons if {
  reasons := [
    {
      "code": "VELOCITY_SPIKE",
      "source": "feature",
      "value": velocity_component,
      "threshold": 0.45,
      "contribution": 0.35 * velocity_component,
      "evidence_ref": sprintf("velocity:%s", [feature.feature_version]),
    },
    {
      "code": "ANOMALY_SCORE",
      "source": "feature",
      "value": anomaly_component,
      "threshold": 0.50,
      "contribution": 0.30 * anomaly_component,
      "evidence_ref": sprintf("feature:%s", [feature.feature_version]),
    },
    {
      "code": "GRAPH_RING_SCORE",
      "source": "graph",
      "value": graph.graph_score,
      "threshold": 0.50,
      "contribution": 0.25 * graph.graph_score,
      "evidence_ref": sprintf("ring:%s", [graph.ring_id]),
    },
  ]
}

clamp(value) := out if {
  value < 0
  out := 0
} else := out if {
  value > 1
  out := 1
} else := out if {
  out := value
}
