"""Simula medições de rede em tempo real para o tópico Kafka ``network_events``.

Inclui ~10%% duplicados e, por defeito, alguns nulls/campos inválidos para testar
cleansing na silver (MERGE SQL). Logs estruturados para Grafana/Loki (``event_sent``).

Requer: pip install -r python_scripts/requirements.txt

Uso (com Redpanda exposto em localhost:19092):
    python producer_network_events.py
    python producer_network_events.py --no-nulls
    python producer_network_events.py --no-loki
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from kafka import KafkaProducer

_REPO_ROOT = Path(__file__).resolve().parents[1]
_FLYTE_WORKFLOWS = _REPO_ROOT / "flyte-workflows"
if str(_FLYTE_WORKFLOWS) not in sys.path:
    sys.path.insert(0, str(_FLYTE_WORKFLOWS))

TOPIC = "network_events"
BROKER = "localhost:19092"
DEFAULT_LOKI_URL = "http://localhost:3100/loki/api/v1/push"

PHONE_PREFIXES = ("912", "913", "914", "915", "916", "917", "918", "919")
NETWORK_TYPES = ("LTE", "NR", "UMTS", "GSM")
MESSY_NETWORK_TYPES = ("", "lte ", "5G/LTE", "unknown")
LEIRIA_LAT = 39.74
LEIRIA_LON = -8.81

NULL_RATES = {
    "rsrp": 0.06,
    "sinr": 0.05,
    "gps": 0.04,
    "phone": 0.03,
    "network_type": 0.04,
    "event_time": 0.03,
    "rf_outlier": 0.03,
}


def _configure_logging(*, use_loki: bool, loki_url: str) -> logging.Logger:
    if use_loki:
        os.environ.setdefault("LOKI_URL", loki_url)
        from loki_logging import get_logger

        return get_logger("producer_network_events")

    logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(name)s] %(message)s")
    return logging.getLogger("producer_network_events")


def _build_payload(inject_nulls: bool) -> dict:
    phone = f"{random.choice(PHONE_PREFIXES)}{random.randint(100000, 999999)}"
    device_id = f"DEV_{random.randint(1000, 9999)}"
    network_type = random.choice(NETWORK_TYPES)
    event_id = f"EVT_{random.getrandbits(32):08x}"
    rsrp = round(random.uniform(-120.0, -70.0), 1)
    sinr = round(random.uniform(-5.0, 25.0), 1)
    #event_time = (datetime.now(timezone.utc) - timedelta(hours=4)).isoformat()
    event_time = datetime.now(timezone.utc).isoformat()
    lat = round(LEIRIA_LAT + random.uniform(-0.05, 0.05), 5)
    lon = round(LEIRIA_LON + random.uniform(-0.05, 0.05), 5)

    if inject_nulls:
        if random.random() < NULL_RATES["phone"]:
            phone = random.choice(("", "123", "INVALID"))
        if random.random() < NULL_RATES["network_type"]:
            network_type = random.choice(MESSY_NETWORK_TYPES)
        if random.random() < NULL_RATES["event_time"]:
            event_time = random.choice(("", "not-a-date", "2026/99/99 25:00:00"))
        if random.random() < NULL_RATES["gps"]:
            lat = None
            lon = None
        if random.random() < NULL_RATES["rf_outlier"]:
            rsrp = round(random.uniform(-200.0, -150.0), 1)
        if random.random() < NULL_RATES["rsrp"]:
            rsrp = None
        if random.random() < NULL_RATES["sinr"]:
            sinr = None

    payload: dict = {
        "event_id": event_id,
        "phone_number": phone,
        "device_id": device_id,
        "network_type": network_type,
        "event_time": event_time,
    }
    if rsrp is not None:
        payload["rsrp"] = rsrp
    if sinr is not None:
        payload["sinr"] = sinr
    if lat is not None:
        payload["latitude"] = lat
    if lon is not None:
        payload["longitude"] = lon

    return payload


def _log_event_sent(logger: logging.Logger, payload: dict, *, duplicate: bool) -> None:
    logger.info(
        "event_sent event_id=%s dup=%s topic=%s network_type=%s phone=%s rsrp=%s sinr=%s",
        payload.get("event_id"),
        duplicate,
        TOPIC,
        payload.get("network_type"),
        payload.get("phone_number"),
        payload.get("rsrp"),
        payload.get("sinr"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka network_events producer")
    parser.add_argument("--broker", default=BROKER)
    parser.add_argument(
        "--no-nulls",
        action="store_true",
        help="Desativar injeção de nulls/campos inválidos (mantém duplicados)",
    )
    parser.add_argument(
        "--no-loki",
        action="store_true",
        help="Só consola; não enviar logs para Loki/Grafana",
    )
    parser.add_argument(
        "--loki-url",
        default=os.environ.get("LOKI_URL", DEFAULT_LOKI_URL),
        help=f"URL push Loki (default: {DEFAULT_LOKI_URL})",
    )
    args = parser.parse_args()
    inject_nulls = not args.no_nulls
    use_loki = not args.no_loki

    logger = _configure_logging(use_loki=use_loki, loki_url=args.loki_url)

    producer = KafkaProducer(
        bootstrap_servers=[args.broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        enable_idempotence=True,
    )

    logger.info(
        "producer_started topic=%s broker=%s nulls=%s loki=%s",
        TOPIC,
        args.broker,
        inject_nulls,
        use_loki,
    )
    print(
        f"Producer → '{TOPIC}' @ {args.broker} "
        f"(nulls={'on' if inject_nulls else 'off'}, dup~10%, loki={'on' if use_loki else 'off'})"
    )

    try:
        while True:
            payload = _build_payload(inject_nulls)
            event_id = payload["event_id"]

            if random.random() < 0.10:
                producer.send(TOPIC, payload)
                _log_event_sent(logger, payload, duplicate=True)
                print(f"  [DUP] Reenvio do evento {event_id}")

            producer.send(TOPIC, payload)
            _log_event_sent(logger, payload, duplicate=False)
            print(
                f"Enviado: {payload.get('network_type')} | "
                f"{payload.get('phone_number')} | "
                f"RSRP={payload.get('rsrp')} | SINR={payload.get('sinr')} | "
                f"GPS=({payload.get('latitude')},{payload.get('longitude')}) | "
                f"{payload.get('event_time')}"
            )
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        logger.info("producer_stopped")
        print("Producer terminado.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
