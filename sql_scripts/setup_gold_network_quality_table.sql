-- DDL alinhado com contratos v2 (snake_case). Correr ensure_gold_layer_environment ou build_gold_network_quality.
CREATE SCHEMA IF NOT EXISTS iceberg.gold
WITH (location = 's3a://warehouse/gold/');

CREATE TABLE IF NOT EXISTS iceberg.gold.network_quality_daily (
    gold_row_id BIGINT NOT NULL,
    data_hora TIMESTAMP(3),
    zona_leiria VARCHAR,
    latitude_ocorrencia DOUBLE,
    longitude_ocorrencia DOUBLE,
    torre_latitude DOUBLE,
    torre_longitude DOUBLE,
    id_antena_conectada BIGINT,
    estado_antena BOOLEAN,
    distancia_antena_m DOUBLE,
    tecnologia_rede VARCHAR,
    potencia_rsrp DOUBLE,
    qualidade_rsrq DOUBLE,
    ruido_sinr DOUBLE,
    velocidade_downlink DOUBLE,
    telefones_sucesso BIGINT,
    telefones_falha BIGINT,
    telefones_sem_teste BIGINT
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(data_hora)']
);
