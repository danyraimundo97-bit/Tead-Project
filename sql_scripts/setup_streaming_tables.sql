-- Medallion schemas (idempotent; batch pipeline may already have created silver/gold)
CREATE SCHEMA IF NOT EXISTS iceberg.bronze WITH (location = 's3a://warehouse/bronze/');
CREATE SCHEMA IF NOT EXISTS iceberg.silver WITH (location = 's3a://warehouse/silver/');
CREATE SCHEMA IF NOT EXISTS iceberg.gold   WITH (location = 's3a://warehouse/gold/');

-- Bronze: idempotent landing from Kafka (MERGE por event_id no Flyte)
CREATE TABLE IF NOT EXISTS iceberg.bronze.network_events_raw (
    event_id VARCHAR,
    phone_number VARCHAR,
    device_id VARCHAR,
    network_type VARCHAR,
    rsrp DOUBLE,
    sinr DOUBLE,
    latitude DOUBLE,
    longitude DOUBLE,
    event_time_raw VARCHAR,
    ingestion_timestamp TIMESTAMP(6) WITH TIME ZONE,
    ingest_batch_id VARCHAR
)
WITH (
    format = 'PARQUET',
    location = 's3a://warehouse/bronze/network_events_raw/'
);

-- Checkpoint de watermark (atualizado após MERGE bronze → silver bem-sucedido)
CREATE TABLE IF NOT EXISTS iceberg.bronze.streaming_checkpoints (
    pipeline_name VARCHAR,
    last_silver_watermark TIMESTAMP(6) WITH TIME ZONE,
    updated_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    location = 's3a://warehouse/bronze/streaming_checkpoints/'
);

-- Silver: deduplicated stream (chave natural event_id)
CREATE TABLE IF NOT EXISTS iceberg.silver.network_events_clean (
    event_id VARCHAR,
    phone_number VARCHAR,
    device_id VARCHAR,
    network_type VARCHAR,
    rsrp DOUBLE,
    sinr DOUBLE,
    latitude DOUBLE,
    longitude DOUBLE,
    event_time TIMESTAMP(6) WITH TIME ZONE,
    ingested_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    location = 's3a://warehouse/silver/network_events_clean/'
);

-- Gold: agregação horária; chave lógica (bucket_hour, network_type)
CREATE TABLE IF NOT EXISTS iceberg.gold.network_events_hourly (
    bucket_hour TIMESTAMP(6) WITH TIME ZONE,
    network_type VARCHAR,
    event_count BIGINT,
    avg_rsrp DOUBLE,
    avg_sinr DOUBLE,
    poor_signal_count BIGINT,
    updated_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    location = 's3a://warehouse/gold/network_events_hourly/'
);
