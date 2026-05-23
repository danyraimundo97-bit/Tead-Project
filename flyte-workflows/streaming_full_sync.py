"""Workflows streaming (tasks nos módulos separados para logs Loki por passo)."""

from __future__ import annotations

from flytekit import workflow

from streaming_kafka_to_bronze import ingest_kafka_to_bronze
from streaming_silver_to_gold import silver_to_gold_network_events_hourly
from streaming_bronze_to_silver import ingest_bronze_to_silver


@workflow
def jdpt_streaming_full_sync() -> str:
    """Kafka → bronze → silver → gold (pipeline streaming completo)."""
    bronze = ingest_kafka_to_bronze()
    silver = ingest_bronze_to_silver()
    gold = silver_to_gold_network_events_hourly()
    bronze >> silver >> gold
    return gold
