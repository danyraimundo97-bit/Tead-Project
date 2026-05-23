"""Workflow completo streaming: Kafka → bronze → silver → gold."""

from __future__ import annotations

from flytekit import workflow

from streaming_kafka_to_bronze import ingest_kafka_to_bronze
from streaming_silver_to_gold import silver_to_gold_network_events_hourly
from workflows_incremental_streaming import incremental_bronze_to_silver_network_events


@workflow
def jdpt_streaming_full_sync() -> str:
    """Kafka → bronze → silver → gold (pipeline streaming completo)."""
    bronze = ingest_kafka_to_bronze()
    silver = incremental_bronze_to_silver_network_events()
    gold = silver_to_gold_network_events_hourly()
    bronze >> silver >> gold
    return gold
