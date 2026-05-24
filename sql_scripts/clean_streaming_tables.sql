-- Limpa as tabelas do pipeline streaming (bronze → silver → gold).
-- Não afeta tabelas batch (cdr, network_logs, churn_risk_daily, etc.).
--
-- Uso: executar no Trino (http://localhost:8080), após docker compose up.
-- Para reset completo (DROP + ficheiros MinIO), ver flyte-workflows/clean_streaming_tables.py

START TRANSACTION;

DELETE FROM iceberg.gold.network_events_hourly WHERE TRUE;
DELETE FROM iceberg.silver.network_events_clean WHERE TRUE;
DELETE FROM iceberg.bronze.network_events_raw WHERE TRUE;
DELETE FROM iceberg.bronze.streaming_checkpoints WHERE TRUE;

COMMIT;
