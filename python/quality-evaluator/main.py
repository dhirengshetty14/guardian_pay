import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def main() -> int:
    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9092")
    decision_topic = os.getenv("TOPIC_TX_DECISION", "tx.decision")
    group_id = os.getenv("QUALITY_GROUP_ID", f"quality-evaluator-{int(time.time())}")
    max_messages = env_int("QUALITY_MAX_MESSAGES", 200_000)
    poll_timeout = env_float("QUALITY_POLL_TIMEOUT_SEC", 1.0)
    idle_round_limit = env_int("QUALITY_IDLE_ROUNDS", 8)
    min_precision = env_float("QUALITY_MIN_PRECISION", -1.0)
    min_recall = env_float("QUALITY_MIN_RECALL", -1.0)
    output_path = os.getenv("QUALITY_OUTPUT_PATH", "").strip()
    predict_review_as_fraud = os.getenv("QUALITY_REVIEW_IS_FRAUD", "true").lower() == "true"

    positive_decisions = {"DECLINE"}
    if predict_review_as_fraud:
        positive_decisions.add("REVIEW")

    consumer = Consumer(
        {
            "bootstrap.servers": kafka_brokers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([decision_topic])

    confusion = Counter({"tp": 0, "fp": 0, "tn": 0, "fn": 0})
    decision_distribution = Counter()

    consumed_messages = 0
    evaluated_records = 0
    parse_errors = 0
    skipped_no_label = 0
    skipped_unknown_label = 0
    idle_rounds = 0

    try:
        while consumed_messages < max_messages:
            msg = consumer.poll(poll_timeout)
            if msg is None:
                idle_rounds += 1
                if idle_rounds >= idle_round_limit:
                    break
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Kafka consumer error: {msg.error()}", file=sys.stderr)
                continue

            idle_rounds = 0
            consumed_messages += 1

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception:
                parse_errors += 1
                continue

            decision = str(payload.get("decision", "")).upper()
            if decision:
                decision_distribution[decision] += 1

            label = payload.get("label")
            if not isinstance(label, str):
                skipped_no_label += 1
                continue

            label_norm = label.lower()
            if label_norm not in {"fraud", "normal"}:
                skipped_unknown_label += 1
                continue

            predicted_fraud = decision in positive_decisions
            actual_fraud = label_norm == "fraud"

            if predicted_fraud and actual_fraud:
                confusion["tp"] += 1
            elif predicted_fraud and not actual_fraud:
                confusion["fp"] += 1
            elif not predicted_fraud and actual_fraud:
                confusion["fn"] += 1
            else:
                confusion["tn"] += 1

            evaluated_records += 1
    finally:
        consumer.close()

    tp = confusion["tp"]
    fp = confusion["fp"]
    tn = confusion["tn"]
    fn = confusion["fn"]

    precision = safe_div(tp, tp + fp)
    recall = safe_div(tp, tp + fn)
    f1 = safe_div(2 * precision * recall, precision + recall)
    accuracy = safe_div(tp + tn, tp + fp + tn + fn)
    coverage = safe_div(evaluated_records, consumed_messages)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "topic": decision_topic,
        "kafka_brokers": kafka_brokers,
        "consumed_messages": consumed_messages,
        "evaluated_records": evaluated_records,
        "coverage": round(coverage, 6),
        "parse_errors": parse_errors,
        "skipped_no_label": skipped_no_label,
        "skipped_unknown_label": skipped_unknown_label,
        "decision_distribution": dict(decision_distribution),
        "confusion_matrix": {"tp": tp, "fp": fp, "tn": tn, "fn": fn},
        "metrics": {
            "precision": round(precision, 6),
            "recall": round(recall, 6),
            "f1": round(f1, 6),
            "accuracy": round(accuracy, 6),
        },
        "positive_decisions": sorted(list(positive_decisions)),
        "thresholds": {
            "min_precision": None if min_precision < 0 else min_precision,
            "min_recall": None if min_recall < 0 else min_recall,
        },
    }

    if output_path:
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
            handle.write("\n")

    print(json.dumps(report, indent=2))

    failed_thresholds = []
    if min_precision >= 0 and precision < min_precision:
        failed_thresholds.append(
            f"precision={precision:.4f} below min_precision={min_precision:.4f}"
        )
    if min_recall >= 0 and recall < min_recall:
        failed_thresholds.append(f"recall={recall:.4f} below min_recall={min_recall:.4f}")

    if failed_thresholds:
        print("Quality gates failed: " + "; ".join(failed_thresholds), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
