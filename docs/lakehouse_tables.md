# Tead Lakehouse — Fluxos de dados (Mermaid)

**Diagramas ER (entidade–relação):** ver **[lakehouse_er.md](lakehouse_er.md)** — é o documento principal para tabelas, PK/FK e cardinalidade.

Fontes: `sql_scripts/setup_streaming_tables.sql`, `flyte-workflows/ensure_pipeline_layers.py`

> **Como renderizar:** preview Markdown no Cursor, ou cola um `.mmd` em [mermaid.live](https://mermaid.live) (não coles o `.md` inteiro).

Ficheiros `.mmd`:

| Fluxo | ER |
|-------|-----|
| [streaming-flow.mmd](diagrams/streaming-flow.mmd) | [streaming-er.mmd](diagrams/streaming-er.mmd) |
| [batch-flow.mmd](diagrams/batch-flow.mmd) | [batch-er.mmd](diagrams/batch-er.mmd), [batch-er-silver.mmd](diagrams/batch-er-silver.mmd), [batch-er-gold.mmd](diagrams/batch-er-gold.mmd) |
| — | [lakehouse-er-overview.mmd](diagrams/lakehouse-er-overview.mmd) |

## Streaming — fluxo de dados

```mermaid
flowchart LR
  producer["producer_network_events.py"]
  redpanda[("Redpanda network_events")]
  kafka["kafka.default.network_events"]
  bronze["bronze.network_events_raw"]
  silver["silver.network_events_clean"]
  gold["gold.network_events_hourly"]
  ckpt["streaming_checkpoints"]

  producer -->|JSON| redpanda
  redpanda -->|Trino kafka catalog| kafka
  kafka -->|MERGE event_id| bronze
  bronze -->|MERGE event_id| silver
  silver -->|DELETE INSERT 7d| gold
  ckpt -.->|watermark| silver
```

## Streaming — modelo de tabelas

```mermaid
erDiagram
  kafka_network_events ||--o{ bronze_network_events_raw : merges
  bronze_network_events_raw ||--o| silver_network_events_clean : merges
  silver_network_events_clean }o--|| gold_network_events_hourly : aggregates

  kafka_network_events {
    string event_id PK
    string phone_number
    string device_id
    string network_type
    float rsrp
    float sinr
    float latitude
    float longitude
    string event_time
  }

  bronze_network_events_raw {
    string event_id PK
    string phone_number
    string device_id
    string network_type
    float rsrp
    float sinr
    float latitude
    float longitude
    string event_time_raw
    string ingestion_timestamp
    string ingest_batch_id
  }

  streaming_checkpoints {
    string pipeline_name PK
    string last_silver_watermark
    string updated_at
  }

  silver_network_events_clean {
    string event_id PK
    string phone_number
    string device_id
    string network_type
    float rsrp
    float sinr
    float latitude
    float longitude
    string event_time
    string ingested_at
  }

  gold_network_events_hourly {
    string bucket_hour
    string network_type
    int event_count
    float avg_rsrp
    float avg_sinr
    int poor_signal_count
    string updated_at
  }
```

> `kafka.default.network_events` não é Iceberg — tópico Redpanda (`trino/etc/kafka/network_events.json`).  
> `streaming_checkpoints` guarda watermark (ver fluxo acima). PK composta em gold: `(bucket_hour, network_type)`.

## Batch — fluxo (Medallion CSV)

```mermaid
flowchart TB
  subgraph bronze_minio ["MinIO bronze CSV"]
    b_logs["network_logs CSV"]
    b_cdr["cdr_customers CSV"]
    b_tests["call_tests CSV"]
    b_towers["towers CSV"]
  end

  subgraph silver_iceberg ["iceberg.silver"]
    s_logs["network_logs"]
    s_cdr["cdr_customers"]
    s_tests["call_tests"]
    s_towers["towers"]
  end

  subgraph gold_iceberg ["iceberg.gold"]
    g_quality["network_quality_daily"]
    g_churn["churn_risk_daily"]
  end

  b_logs --> s_logs
  b_cdr --> s_cdr
  b_tests --> s_tests
  b_towers --> s_towers

  s_logs --> g_quality
  s_towers --> g_quality
  s_cdr --> g_churn
  s_tests --> g_churn
  s_logs --> g_churn
```

## Batch — modelo de tabelas (principais)

```mermaid
erDiagram
  silver_network_logs ||--o{ gold_network_quality_daily : builds
  silver_towers ||--o{ gold_network_quality_daily : joins
  silver_cdr_customers ||--o{ gold_churn_risk_daily : builds
  silver_call_tests ||--o{ gold_churn_risk_daily : builds
  silver_network_logs ||--o{ gold_churn_risk_daily : builds
  silver_cdr_customers ||--o{ silver_network_logs : phone_number
  silver_cdr_customers ||--o{ silver_call_tests : phone_number

  silver_network_logs {
    int silver_row_id PK
    string timestamp_log
    string phone_number
    float rsrp
    float sinr
    float latitude
    float longitude
  }

  silver_cdr_customers {
    int silver_row_id PK
    string phone_number
    boolean churn
    float day_mins
    int custserv_calls
  }

  silver_call_tests {
    int silver_row_id PK
    string phone_number
    string date_of_test
    float mos
    boolean result
  }

  silver_towers {
    int silver_row_id PK
    int mcc
    int net
    int area
    int cell
    float lat
    float lon
  }

  gold_network_quality_daily {
    int gold_row_id PK
    string Data_Hora
    string Telefone
    float Potencia_RSRP
    int ID_Antena_Conectada
    float Distancia_Antena_m
  }

  gold_churn_risk_daily {
    int gold_row_id PK
    string Data_Referencia
    string Telefone
    boolean Desistencia
    float Receita_Em_Risco
    boolean Afetado_Tempestade
  }
```

## Legenda

| Símbolo / termo | Significado |
|-----------------|-------------|
| PK | Chave primária ou natural (`event_id`; gold: `bucket_hour` + `network_type`) |
| MERGE | Escrita idempotente no streaming |
| Linha tracejada (fluxo) | Metadata / watermark, sem FK no Iceberg |
| phone_number | Relação lógica entre tabelas batch |
