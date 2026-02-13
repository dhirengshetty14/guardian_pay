import ipaddress
import random
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional
from uuid import uuid4

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


class SimulationRequest(BaseModel):
    scenario: str = Field(default="baseline")
    count: int = Field(default=1000, ge=1, le=500000)
    rate_per_sec: int = Field(default=500, ge=1, le=50000)


class TransactionEvent(BaseModel):
    tx_id: str
    event_time: datetime
    account_id: str
    card_id: str
    merchant_id: str
    amount: float
    currency: str
    country: str
    device_id: str
    ip: str
    mcc: str
    channel: str
    is_card_present: bool
    user_agent: str
    label: Optional[str] = None


KAFKA_BROKERS = "localhost:9092"
TOPIC_TX_RAW = "tx.raw"

app = FastAPI(title="guardianpay-simulator", version="0.1.0")
producer: Optional[Producer] = None


def _delivery_report(err, msg) -> None:
    if err is not None:
        print(f"delivery failed for {msg.key()}: {err}")


@app.on_event("startup")
def startup() -> None:
    global producer, KAFKA_BROKERS, TOPIC_TX_RAW
    import os

    KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", KAFKA_BROKERS)
    TOPIC_TX_RAW = os.getenv("TOPIC_TX_RAW", TOPIC_TX_RAW)
    producer = Producer({"bootstrap.servers": KAFKA_BROKERS})
    random.seed(42)


@app.get("/v1/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "service": "simulator"}


@app.post("/internal/run")
def run_simulation(payload: SimulationRequest) -> Dict[str, object]:
    if producer is None:
        raise HTTPException(status_code=500, detail="producer not initialized")

    scenario = payload.scenario.lower()
    if scenario not in {
        "baseline",
        "card_testing",
        "ring_expansion",
        "account_takeover",
        "bust_out",
    }:
        raise HTTPException(status_code=400, detail=f"unknown scenario: {payload.scenario}")

    sent = 0
    started = time.time()
    batch_size = max(1, payload.rate_per_sec // 10)
    sleep_sec = 0.1

    for idx in range(payload.count):
        tx = generate_transaction(idx, scenario, payload.count)
        body = tx.model_dump_json()
        producer.produce(
            TOPIC_TX_RAW,
            key=tx.tx_id,
            value=body,
            callback=_delivery_report,
        )
        sent += 1
        if idx % batch_size == 0:
            producer.poll(0)
            time.sleep(sleep_sec)

    producer.flush(timeout=20)
    duration = time.time() - started
    return {
        "status": "accepted",
        "scenario": scenario,
        "sent": sent,
        "duration_sec": round(duration, 2),
        "rate_per_sec": round(sent / max(duration, 0.001), 2),
    }


def generate_transaction(i: int, scenario: str, total: int) -> TransactionEvent:
    account_id = f"acct_{random.randint(1, 45000):06d}"
    card_id = f"card_{random.randint(1, 70000):06d}"
    merchant_id = f"mer_{random.randint(1, 5000):05d}"
    country = random.choice(["US", "CA", "GB", "DE", "SG"])
    mcc = random.choice(["5411", "5812", "5732", "5999", "4511", "4789"])
    channel = random.choice(["web", "mobile", "pos"])
    user_agent = random.choice(
        [
            "Mozilla/5.0 Chrome/125.0",
            "Mozilla/5.0 Mobile Safari/17.2",
            "Mozilla/5.0 Firefox/123.0",
        ]
    )
    amount = round(max(1.0, random.gauss(65, 35)), 2)
    is_card_present = channel == "pos"
    label = "normal"

    if scenario == "card_testing":
        if i > int(total * 0.6):
            amount = round(random.uniform(1.0, 5.0), 2)
            card_id = f"card_test_{random.randint(1, 200):04d}"
            merchant_id = "mer_test_0001"
            label = "fraud"
    elif scenario == "ring_expansion":
        if i > int(total * 0.4):
            device_id = f"dev_ring_{random.randint(1, 4):03d}"
            ip = str(ipaddress.IPv4Address(0xC0A80000 + random.randint(1, 15)))
            account_id = f"acct_ring_{random.randint(1, 600):05d}"
            amount = round(random.uniform(120, 600), 2)
            label = "fraud"
            return _build_tx(
                account_id,
                card_id,
                merchant_id,
                amount,
                country,
                device_id,
                ip,
                mcc,
                channel,
                is_card_present,
                user_agent,
                label,
            )
    elif scenario == "account_takeover":
        if i > int(total * 0.7):
            account_id = f"acct_ato_{random.randint(1, 1000):04d}"
            country = random.choice(["RU", "NG", "BR", "UA"])
            device_id = f"dev_new_{random.randint(1, 100000):05d}"
            ip = str(ipaddress.IPv4Address(0x0A000000 + random.randint(1, 65535)))
            amount = round(random.uniform(300, 1800), 2)
            label = "fraud"
            return _build_tx(
                account_id,
                card_id,
                merchant_id,
                amount,
                country,
                device_id,
                ip,
                mcc,
                channel,
                False,
                user_agent,
                label,
            )
    elif scenario == "bust_out":
        if i > int(total * 0.85):
            account_id = f"acct_bust_{random.randint(1, 250):04d}"
            amount = round(random.uniform(900, 4500), 2)
            merchant_id = random.choice(["mer_lux_0001", "mer_jewel_0002", "mer_travel_0003"])
            label = "fraud"

    device_id = f"dev_{random.randint(1, 30000):05d}"
    ip = str(ipaddress.IPv4Address(0x0A000000 + random.randint(1, 65535)))
    return _build_tx(
        account_id,
        card_id,
        merchant_id,
        amount,
        country,
        device_id,
        ip,
        mcc,
        channel,
        is_card_present,
        user_agent,
        label,
    )


def _build_tx(
    account_id: str,
    card_id: str,
    merchant_id: str,
    amount: float,
    country: str,
    device_id: str,
    ip: str,
    mcc: str,
    channel: str,
    is_card_present: bool,
    user_agent: str,
    label: str,
) -> TransactionEvent:
    return TransactionEvent(
        tx_id=str(uuid4()),
        event_time=datetime.now(timezone.utc),
        account_id=account_id,
        card_id=card_id,
        merchant_id=merchant_id,
        amount=amount,
        currency="USD",
        country=country,
        device_id=device_id,
        ip=ip,
        mcc=mcc,
        channel=channel,
        is_card_present=is_card_present,
        user_agent=user_agent,
        label=label,
    )
