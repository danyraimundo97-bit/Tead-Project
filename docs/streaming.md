# Pipeline streaming (Kafka / Redpanda)

Este ramo complementa o pipeline **batch** (CSV → MinIO → Flyte) com ingestão **em tempo quase real** via Kafka, alinhado à stack TEAD 2.0 v1.3 do professor.

## Arquitetura

```
Producer (host) ──► Redpanda (network_events)
                         │
                         ▼
              Trino catálogo kafka
                         │
         Flyte: kafka → bronze (MERGE por event_id)
                         │
         Flyte: bronze → silver (MERGE + watermark + lookback)
                         │
         Flyte: silver → gold (DELETE janela + INSERT)
```

O pipeline batch existente (`jdpt_lakehouse_pipeline`) **não é substituído** — convive com este fluxo.

## Idempotência e retries

### Garantias por camada

| Camada | Mecanismo | Reexecutar workflow |
|--------|-----------|---------------------|
| **Bronze** | `MERGE` em `event_id` (só `WHEN NOT MATCHED`) | Seguro: não duplica linhas |
| **Silver** | `MERGE` em `event_id` + dedup `ROW_NUMBER` na fonte + lookback 1h | Seguro |
| **Gold** | `DELETE` da janela 7d + `INSERT` (chave `bucket_hour`, `network_type`) | Seguro |
| **Checkpoint** | `iceberg.bronze.streaming_checkpoints` atualizado após MERGE silver | Recupera falhas parciais |

### Retries

- **Flyte:** tasks streaming com `retries=3` e `timeout=15min`.
- **Trino:** `execute_with_retry` em [`streaming_trino_client.py`](../flyte-workflows/streaming_trino_client.py) (backoff 2s, 5s, 10s) para erros transitórios (timeout, rede).
- **Producer:** `acks=all`, `retries=3`, `enable_idempotence=True` — reduz duplicados por reenvio no broker. Os ~10% duplicados **simulados** no script servem para testar o MERGE na silver.

### Ordem recomendada

1. `streaming_kafka_to_bronze_workflow`
2. `jdpt_streaming_incremental_sync` (ou `jdpt_streaming_full_sync` que inclui gold)
3. `jdpt_streaming_quality_check` (opcional)

### Duplicados: producer vs retry

- **Producer (`[DUP]`):** mesmo `event_id` enviado duas vezes ao tópico — bronze ignora o segundo (MERGE); silver também.
- **Retry Flyte:** após idempotência bronze/gold, reexecutar uma task não deve aumentar contagens.

### Queries de verificação manual (Trino)

```sql
-- Duplicados bronze (esperado: 0)
SELECT COUNT(*) - COUNT(DISTINCT event_id) AS dupes
FROM iceberg.bronze.network_events_raw;

-- Duplicados silver (esperado: 0)
SELECT COUNT(*) - COUNT(DISTINCT event_id) AS dupes
FROM iceberg.silver.network_events_clean;

-- Checkpoint atual
SELECT * FROM iceberg.bronze.streaming_checkpoints;

-- Órfãos recentes (bronze válido sem silver)
SELECT COUNT(DISTINCT b.event_id)
FROM iceberg.bronze.network_events_raw b
LEFT JOIN iceberg.silver.network_events_clean s ON b.event_id = s.event_id
WHERE s.event_id IS NULL
  AND b.ingestion_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
  AND b.rsrp IS NOT NULL;
```

## 1. Subir a stack

```bash
docker compose up -d --build
```

Confirme que `redpanda` e `trino` estão `Up`. O catálogo `kafka` deve aparecer:

```bash
docker compose exec trino trino --execute "SHOW CATALOGS;"
```

## 2. Criar tabelas Iceberg (streaming)

No Trino UI (`http://localhost:8080`) ou via CLI:

1. `sql_scripts/setup_streaming_tables.sql` — instalação nova
2. `sql_scripts/migrate_streaming_dedup.sql` — se já tinha tabelas sem `ingest_batch_id` / checkpoint

## 3. Simular eventos de rede

No host (fora do Docker):

```bash
pip install -r python_scripts/requirements.txt
python python_scripts/producer_network_events.py
```

O broker exposto é `localhost:19092` (porta externa do Redpanda).

## 4. Validar leitura Kafka no Trino

```sql
SELECT * FROM kafka.default.network_events LIMIT 10;
```

## 5. Workflows Flyte (remoto)

| Workflow | Ficheiro | Função |
|----------|----------|--------|
| `streaming_kafka_to_bronze_workflow` | `streaming_kafka_to_bronze.py` | MERGE idempotente Kafka → bronze |
| `jdpt_streaming_incremental_sync` | `workflows_incremental_streaming.py` | MERGE incremental bronze → silver |
| `jdpt_streaming_batch_gold_sync` | `workflows_incremental_streaming.py` | Agrega silver → gold |
| `jdpt_streaming_full_sync` | `workflows_incremental_streaming.py` | Bronze → silver → gold |
| `jdpt_streaming_quality_check` | `avaliar_streaming.py` | QA duplicados e órfãos |

Exemplo:

```bash
pyflyte run --remote flyte-workflows/streaming_kafka_to_bronze.py streaming_kafka_to_bronze_workflow
pyflyte run --remote flyte-workflows/workflows_incremental_streaming.py jdpt_streaming_full_sync
pyflyte run --remote flyte-workflows/avaliar_streaming.py jdpt_streaming_quality_check
```

## 6. Ligação ao domínio Leiria

- O **batch** continua a alimentar `network_logs`, CDR, torres e gold `network_quality_daily` / `churn_risk_daily`.
- O **streaming** simula medições de campo (`rsrp`, `sinr`, coordenadas na zona de Leiria) para monitorização contínua e testes de idempotência.

Para dashboards Superset, pode registar `iceberg.gold.network_events_hourly` como dataset adicional.

## Erros Flyte (`TypeError: bad argument type`)

Se o **2.º workflow** falhar com `TypeError: bad argument type for built-in operation` no `pyflyte-execute`, o Flyte **escondeu** o erro real do Trino. Ver os logs brutos do pod ou voltar a correr após `pyflyte register` com o código atualizado (as tasks re-levantam `RuntimeError` com a mensagem SQL).

**`MERGE_TARGET_ROW_MULTIPLE_MATCHES` no kafka→bronze:** o tópico Kafka tem o mesmo `event_id` várias vezes (producer ~10% duplicados). O workflow deduplica com `ROW_NUMBER` antes do MERGE. Se a bronze já tiver duplicados de runs antigas com `INSERT`, descomenta a limpeza em `migrate_streaming_dedup.sql`.

Causas frequentes do 2.º passo (`jdpt_streaming_incremental_sync` / `full_sync`):

1. Não correr `sql_scripts/setup_streaming_tables.sql` (falta `silver.network_events_clean` ou `streaming_checkpoints`).
2. Bronze vazio — correr primeiro `streaming_kafka_to_bronze_workflow` com o producer ativo.
3. Coluna `ingest_batch_id` em falta na bronze — correr `migrate_streaming_dedup.sql`.

Ordem correta: **setup SQL** → **producer** → **kafka_to_bronze** → **incremental** ou **full_sync**.

## Portas e credenciais

| Serviço | Host (Flyte tasks) | Host (máquina local) |
|---------|-------------------|----------------------|
| Kafka/Redpanda | — | `localhost:19092` |
| Trino | `host.docker.internal:8080` | `localhost:8080` |
| MinIO | `host.docker.internal:9000` | `localhost:9000` |
