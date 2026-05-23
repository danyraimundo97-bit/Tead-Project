# Tead Lakehouse — Diagrama ER (entidade–relação)

Diagramas **só de tabelas** (atributos + cardinalidade). Para fluxos Kafka/Flyte ver [lakehouse_tables.md](lakehouse_tables.md).

**Render:** preview deste `.md` no Cursor, ou colar um `.mmd` de [diagrams/](diagrams/) em [mermaid.live](https://mermaid.live) (só o conteúdo do ficheiro, sem `#`).

---

## 1. Streaming — ER completo

Catálogos: `kafka` (tópico) → `iceberg.bronze` → `iceberg.silver` → `iceberg.gold`.

```mermaid
erDiagram
  KAFKA_NETWORK_EVENTS ||--o{ BRONZE_NETWORK_EVENTS_RAW : "0..N por event_id"
  BRONZE_NETWORK_EVENTS_RAW ||--o| SILVER_NETWORK_EVENTS_CLEAN : "0..1 por event_id"
  SILVER_NETWORK_EVENTS_CLEAN }o--|| GOLD_NETWORK_EVENTS_HOURLY : "N agrega por hora e rede"
  STREAMING_CHECKPOINTS ||--o| SILVER_NETWORK_EVENTS_CLEAN : "controla watermark"

  KAFKA_NETWORK_EVENTS {
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

  BRONZE_NETWORK_EVENTS_RAW {
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

  STREAMING_CHECKPOINTS {
    string pipeline_name PK
    string last_silver_watermark
    string updated_at
  }

  SILVER_NETWORK_EVENTS_CLEAN {
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

  GOLD_NETWORK_EVENTS_HOURLY {
    string bucket_hour PK
    string network_type PK
    int event_count
    float avg_rsrp
    float avg_sinr
    int poor_signal_count
    string updated_at
  }
```

| Entidade | Schema | Persistência |
|----------|--------|----------------|
| `KAFKA_NETWORK_EVENTS` | `kafka.default` | Redpanda (não MinIO) |
| `BRONZE_NETWORK_EVENTS_RAW` | `iceberg.bronze` | Parquet MinIO |
| `STREAMING_CHECKPOINTS` | `iceberg.bronze` | Parquet MinIO |
| `SILVER_NETWORK_EVENTS_CLEAN` | `iceberg.silver` | Parquet MinIO |
| `GOLD_NETWORK_EVENTS_HOURLY` | `iceberg.gold` | Parquet MinIO |

---

## 2. Batch — ER silver (domínio telecom)

```mermaid
erDiagram
  SILVER_CDR_CUSTOMERS ||--o{ SILVER_NETWORK_LOGS : "phone_number"
  SILVER_CDR_CUSTOMERS ||--o{ SILVER_CALL_TESTS : "phone_number"
  SILVER_TOWERS ||--o{ SILVER_NETWORK_LOGS : "join espacial logico"

  SILVER_CDR_CUSTOMERS {
    int silver_row_id PK
    string phone_number
    int account_length
    int vmail_message
    float day_mins
    int day_calls
    float day_charge
    float eve_mins
    int eve_calls
    float eve_charge
    float night_mins
    int night_calls
    float night_charge
    float intl_mins
    int intl_calls
    float intl_charge
    int custserv_calls
    boolean churn
  }

  SILVER_NETWORK_LOGS {
    int silver_row_id PK
    string timestamp_log
    string devicemake
    string devicemodel
    string network_provider
    boolean nt_ohe_lte
    boolean nt_ohe_gsm
    boolean nt_ohe_umts
    boolean nt_ohe_nr
    float rsrp
    float rsrq
    float sinr
    float pci
    float downlink_mbps
    float uplink_mbps
    float velocity_kmh
    float latitude
    float longitude
    string phone_number FK
  }

  SILVER_CALL_TESTS {
    int silver_row_id PK
    string date_of_test
    float signal_dbm
    float speed_m_s
    float distance_from_site_m
    float duration_s
    float setup_time_s
    boolean result
    float mos
    string phone_number FK
    boolean tech_ohe_lte
    boolean tech_ohe_nr
  }

  SILVER_TOWERS {
    int silver_row_id PK
    int mcc
    int net
    int area
    int cell
    int unit
    float lon
    float lat
    int range_m
    int samples
    string snapshot_date
    boolean status
    boolean radio_ohe_lte
    boolean radio_ohe_nr
  }
```

> `phone_number` é unique no negócio após dedup; FK = ligação lógica (sem constraint Iceberg).

---

## 3. Batch — ER gold (data products)

```mermaid
erDiagram
  SILVER_NETWORK_LOGS ||--o{ GOLD_NETWORK_QUALITY_DAILY : "agrega e enriquece"
  SILVER_TOWERS ||--o{ GOLD_NETWORK_QUALITY_DAILY : "antena mais proxima"
  SILVER_CDR_CUSTOMERS ||--o{ GOLD_CHURN_RISK_DAILY : "perfil cliente"
  SILVER_CALL_TESTS ||--o{ GOLD_CHURN_RISK_DAILY : "MOS e testes"
  SILVER_NETWORK_LOGS ||--o{ GOLD_CHURN_RISK_DAILY : "drops e tempestade"

  GOLD_NETWORK_QUALITY_DAILY {
    int gold_row_id PK
    string Data_Hora
    string Zona_Leiria
    float Latitude_Ocorrencia
    float Longitude_Ocorrencia
    float Torre_Latitude
    float Torre_Longitude
    int ID_Antena_Conectada
    boolean Estado_Antena
    float Distancia_Antena_m
    string Tecnologia_Rede
    float Potencia_RSRP
    float Qualidade_RSRQ
    float Ruido_SINR
    float Velocidade_Downlink
    int Telefones_Sucesso
    int Telefones_Falha
    int Telefones_Sem_Teste
  }

  GOLD_CHURN_RISK_DAILY {
    int gold_row_id PK
    date Data_Referencia
    string Telefone FK
    boolean Afetado_Tempestade
    float Receita_Em_Risco
    int Tempo_Subscrito
    int Total_Chamadas_Suporte
    int Total_Drops
    float Qualidade_Audio_MOS
    boolean Desistencia
  }

  SILVER_NETWORK_LOGS {
    int silver_row_id PK
    string phone_number
  }

  SILVER_TOWERS {
    int silver_row_id PK
    float lat
    float lon
  }

  SILVER_CDR_CUSTOMERS {
    int silver_row_id PK
    string phone_number
  }

  SILVER_CALL_TESTS {
    int silver_row_id PK
    string phone_number
  }
```

---

## 4. Vista global (streaming + batch, só entidades principais)

```mermaid
erDiagram
  KAFKA_NETWORK_EVENTS ||--o{ BRONZE_NETWORK_EVENTS_RAW : streaming
  BRONZE_NETWORK_EVENTS_RAW ||--o| SILVER_NETWORK_EVENTS_CLEAN : streaming
  SILVER_NETWORK_EVENTS_CLEAN }o--|| GOLD_NETWORK_EVENTS_HOURLY : streaming

  SILVER_CDR_CUSTOMERS ||--o{ SILVER_NETWORK_LOGS : batch
  SILVER_CDR_CUSTOMERS ||--o{ SILVER_CALL_TESTS : batch
  SILVER_NETWORK_LOGS ||--o{ GOLD_NETWORK_QUALITY_DAILY : batch
  SILVER_TOWERS ||--o{ GOLD_NETWORK_QUALITY_DAILY : batch
  SILVER_CDR_CUSTOMERS ||--o{ GOLD_CHURN_RISK_DAILY : batch
  SILVER_CALL_TESTS ||--o{ GOLD_CHURN_RISK_DAILY : batch
  SILVER_NETWORK_LOGS ||--o{ GOLD_CHURN_RISK_DAILY : batch

  KAFKA_NETWORK_EVENTS {
    string event_id PK
  }
  BRONZE_NETWORK_EVENTS_RAW {
    string event_id PK
  }
  SILVER_NETWORK_EVENTS_CLEAN {
    string event_id PK
  }
  GOLD_NETWORK_EVENTS_HOURLY {
    string bucket_hour PK
    string network_type PK
  }
  SILVER_CDR_CUSTOMERS {
    int silver_row_id PK
    string phone_number
  }
  SILVER_NETWORK_LOGS {
    int silver_row_id PK
    string phone_number
  }
  SILVER_CALL_TESTS {
    int silver_row_id PK
  }
  SILVER_TOWERS {
    int silver_row_id PK
  }
  GOLD_NETWORK_QUALITY_DAILY {
    int gold_row_id PK
  }
  GOLD_CHURN_RISK_DAILY {
    int gold_row_id PK
    string Telefone
  }
```

---

## Legenda ER

| Notação | Significado |
|---------|-------------|
| `PK` | Chave primária (ou natural key no streaming) |
| `FK` | Chave estrangeira **lógica** (MERGE / join; Iceberg não impõe FK) |
| `UK` | Unique no modelo de negócio após transformação |
| `\|\|--o{` | 1 para N |
| `\|\|--o\|` | 1 para 0..1 |
| `}o--\|\|` | N para 1 (muitos eventos → uma linha agregada gold) |

Ficheiros Mermaid só-ER: [streaming-er.mmd](diagrams/streaming-er.mmd), [batch-er.mmd](diagrams/batch-er.mmd), [batch-er-silver.mmd](diagrams/batch-er-silver.mmd), [batch-er-gold.mmd](diagrams/batch-er-gold.mmd), [lakehouse-er-overview.mmd](diagrams/lakehouse-er-overview.mmd).
