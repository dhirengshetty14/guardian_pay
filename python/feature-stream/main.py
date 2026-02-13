import json
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Deque, Dict

from confluent_kafka import Consumer, Producer


@dataclass
class RunningStats:
    n: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def update(self, value: float) -> None:
        self.n += 1
        delta = value - self.mean
        self.mean += delta / self.n
        delta2 = value - self.mean
        self.m2 += delta * delta2

    @property
    def variance(self) -> float:
        return self.m2 / (self.n - 1) if self.n > 1 else 1.0

    @property
    def std(self) -> float:
        return max(self.variance ** 0.5, 1e-6)


class FeatureStream:
    def __init__(self, brokers: str, topic_raw: str, topic_feature: str) -> None:
        self.consumer = Consumer(
            {
                "bootstrap.servers": brokers,
                "group.id": "feature-stream-v1",
                "auto.offset.reset": "earliest",
            }
        )
        self.consumer.subscribe([topic_raw])
        self.producer = Producer({"bootstrap.servers": brokers})
        self.topic_feature = topic_feature

        self.card_window_1m: Dict[str, Deque[float]] = defaultdict(deque)
        self.device_window_5m: Dict[str, Deque[float]] = defaultdict(deque)
        self.ip_window_15m: Dict[str, Deque[float]] = defaultdict(deque)
        self.account_stats: Dict[str, RunningStats] = defaultdict(RunningStats)

    def run(self) -> None:
        print("feature-stream consuming tx.raw and producing tx.feature")
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue
            try:
                tx = json.loads(msg.value().decode("utf-8"))
                feature = self.compute_feature(tx)
                self.producer.produce(
                    self.topic_feature,
                    key=feature["tx_id"],
                    value=json.dumps(feature).encode("utf-8"),
                )
                self.producer.poll(0)
            except Exception as exc:
                print(f"feature processing error: {exc}")

    def compute_feature(self, tx: Dict[str, object]) -> Dict[str, object]:
        now_ts = self._parse_ts(str(tx["event_time"]))
        card_id = str(tx["card_id"])
        device_id = str(tx["device_id"])
        ip = str(tx["ip"])
        account_id = str(tx["account_id"])
        amount = float(tx["amount"])

        velocity_1m = self._update_window(self.card_window_1m[card_id], now_ts, 60.0)
        velocity_5m = self._update_window(self.device_window_5m[device_id], now_ts, 300.0)
        velocity_15m = self._update_window(self.ip_window_15m[ip], now_ts, 900.0)

        stats = self.account_stats[account_id]
        zscore = 0.0 if stats.n < 3 else (amount - stats.mean) / stats.std
        stats.update(amount)

        anomaly_score = self._anomaly_score(velocity_1m, velocity_5m, velocity_15m, zscore)
        return {
            "tx_id": tx["tx_id"],
            "velocity_1m_card": float(velocity_1m),
            "velocity_5m_device": float(velocity_5m),
            "velocity_15m_ip": float(velocity_15m),
            "amount_zscore_1h_account": float(zscore),
            "anomaly_score": float(anomaly_score),
            "feature_version": "feature-stream-v1",
        }

    @staticmethod
    def _update_window(window: Deque[float], now_ts: float, seconds: float) -> int:
        window.append(now_ts)
        cutoff = now_ts - seconds
        while window and window[0] < cutoff:
            window.popleft()
        return len(window)

    @staticmethod
    def _anomaly_score(v1: int, v5: int, v15: int, zscore: float) -> float:
        v_component = min(1.0, (0.50 * v1 + 0.30 * v5 + 0.20 * v15) / 20.0)
        z_component = min(1.0, abs(zscore) / 6.0)
        return max(0.0, min(1.0, 0.65 * v_component + 0.35 * z_component))

    @staticmethod
    def _parse_ts(value: str) -> float:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return time.time()


def main() -> None:
    brokers = os.getenv("KAFKA_BROKERS", "localhost:9092")
    topic_raw = os.getenv("TOPIC_TX_RAW", "tx.raw")
    topic_feature = os.getenv("TOPIC_TX_FEATURE", "tx.feature")
    stream = FeatureStream(brokers, topic_raw, topic_feature)
    stream.run()


if __name__ == "__main__":
    main()
