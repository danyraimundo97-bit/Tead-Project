"""Simula medições de rede em tempo real para o tópico Kafka ``network_events``.

Requer: pip install kafka-python

Uso (com Redpanda exposto em localhost:19092):
    python producer_network_events.py
"""

from __future__ import annotations

import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "network_events"
BROKER = "localhost:19092"

# Prefixos alinhados ao domínio telecom do projeto (Leiria / rede móvel)
PHONE_PREFIXES = ("912", "913", "914", "915", "916", "917", "918", "919")
NETWORK_TYPES = ("LTE", "NR", "UMTS", "GSM")
# Aproximação da área de Leiria para eventos de campo
LEIRIA_LAT = 39.74
LEIRIA_LON = -8.81

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=3,
    enable_idempotence=True,
)

print(f"Producer iniciado. A enviar eventos para o tópico '{TOPIC}' em {BROKER}...")

try:
    while True:
        phone = f"{random.choice(PHONE_PREFIXES)}{random.randint(100000, 999999)}"
        device_id = f"DEV_{random.randint(1000, 9999)}"
        network_type = random.choice(NETWORK_TYPES)
        event_id = f"EVT_{random.getrandbits(32):08x}"

        # RSRP típico em dBm; sinr em dB
        rsrp = round(random.uniform(-120.0, -70.0), 1)
        sinr = round(random.uniform(-5.0, 25.0), 1)
        event_time = datetime.now(timezone.utc).isoformat()

        payload = {
            "event_id": event_id,
            "phone_number": phone,
            "device_id": device_id,
            "network_type": network_type,
            "rsrp": rsrp,
            "sinr": sinr,
            "latitude": round(LEIRIA_LAT + random.uniform(-0.05, 0.05), 5),
            "longitude": round(LEIRIA_LON + random.uniform(-0.05, 0.05), 5),
            "event_time": event_time,
        }

        # ~10% duplicados (como no exemplo do professor) para testar idempotência na silver
        if random.random() < 0.10:
            producer.send(TOPIC, payload)
            print(f"  [DUP] Reenvio do evento {event_id}")

        producer.send(TOPIC, payload)
        print(
            f"Enviado: {network_type} | {phone} | RSRP={rsrp} | "
            f"SINR={sinr} | {event_time}"
        )
        time.sleep(random.uniform(0.5, 2.0))

except KeyboardInterrupt:
    print("Producer terminado.")
finally:
    producer.flush()
    producer.close()
