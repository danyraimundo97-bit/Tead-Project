-- DDL alinhado com contratos v2 (snake_case). Correr ensure_gold_layer_environment ou build_gold_churn_risk.
CREATE SCHEMA IF NOT EXISTS iceberg.gold
WITH (location = 's3a://warehouse/gold/');

CREATE TABLE IF NOT EXISTS iceberg.gold.churn_risk_daily (
    gold_row_id BIGINT NOT NULL,
    data_referencia DATE,
    telefone VARCHAR,
    afetado_tempestade BOOLEAN,
    receita_em_risco DOUBLE,
    tempo_subscrito INTEGER,
    total_chamadas_suporte INTEGER,
    total_drops BIGINT,
    qualidade_audio_mos DOUBLE,
    desistencia BOOLEAN
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['data_referencia']
);
