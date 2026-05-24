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

Tabelas por camada: [lakehouse_tables.md](lakehouse_tables.md).

## Idempotência e retries

### Garantias por camada

| Camada | Mecanismo | Reexecutar workflow |
|--------|-----------|---------------------|
| **Bronze** | `MERGE` em `event_id` (só `WHEN NOT MATCHED`) | Seguro: não duplica linhas |
| **Silver** | Cleansing SQL (normaliza `network_type`, clip RF, valida phone/GPS/timestamp) + `MERGE` + lookback 1h | Seguro |
| **Gold** | `DELETE` da janela 7d + `INSERT` (chave `bucket_hour`, `network_type`) | Seguro |
| **Checkpoint** | `iceberg.bronze.streaming_checkpoints` atualizado após MERGE silver | Recupera falhas parciais |

### Checkpoint (`streaming_checkpoints`) — porquê MERGE e não INSERT?

A tabela guarda **uma linha por pipeline** (`pipeline_name`), com o último ponto processado na silver (`last_silver_watermark`). É um **cursor**, não um histórico.

- **`write_silver_checkpoint`** ([`workflow_functions/streaming_trino_client.py`](../flyte-workflows/workflow_functions/streaming_trino_client.py)) usa `MERGE`: na 1.ª execução faz **INSERT**; nas seguintes faz **UPDATE** da mesma linha com `MAX(ingested_at)` da silver.
- **INSERT em cada run** criaria várias linhas com o mesmo `pipeline_name`. O `read_silver_watermark` faria `fetchone()` sem ordem garantida — risco de ler um watermark antigo e falhar o incremental.
- Alternativa equivalente: `DELETE` + `INSERT` para esse `pipeline_name`. O `MERGE` faz upsert numa só statement.

Fluxo: bronze→silver com sucesso → atualiza checkpoint → próximo run lê o watermark e usa `watermark − 1h` como **`lower_bound`** (lookback de segurança para reprocessar eventos recentes sem duplicar na silver).

### Retries

- **Flyte:** tasks streaming com `retries=3` e `timeout=15min`.
- **Trino:** `execute_with_retry` em [`workflow_functions/streaming_trino_client.py`](../flyte-workflows/workflow_functions/streaming_trino_client.py) (até 3 tentativas, backoff 2s, 5s, 10s).
- **Producer:** `acks=all`, `retries=3`, `enable_idempotence=True`. Simula ~10% **duplicados** e, por defeito, **nulls/campos inválidos** (RSRP/SINR/GPS, telefone, `network_type`, timestamp) para testar cleansing na silver. Usar `--no-nulls` para desativar.

### Ordem recomendada

1. **`jdpt_streaming_full_sync`** — pipeline incremental completo (Kafka → bronze → silver → gold) em [`streaming_full_sync.py`](../flyte-workflows/streaming_full_sync.py)
2. `jdpt_streaming_quality_check` (opcional, QA)

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

### Cleansing bronze → silver

| Regra | Comportamento |
|-------|----------------|
| `phone_number` | TRIM; regex `^9[0-9]{8}$`; inválido → não entra na silver |
| `network_type` | Normaliza para LTE/NR/UMTS/GSM/OTHER |
| `rsrp` / `sinr` | Clip (-140…-60 dBm / -20…40 dB); `rsrp` null → rejeitado |
| `latitude` / `longitude` | Ambos obrigatórios |
| `event_time_raw` | `TRY(from_iso8601_timestamp(...))`; inválido → rejeitado |
| `sinr` | Pode ficar null na silver se o resto for válido |

Linhas em bronze rejeitadas pelo cleanse aparecem como **órfãos** no `jdpt_streaming_quality_check` (esperado com o producer ativo).

## 1. Subir a stack

```bash
docker compose up -d --build
```

Confirme que `redpanda` e `trino` estão `Up`. O catálogo `kafka` deve aparecer:

```bash
docker compose exec trino trino --execute "SHOW CATALOGS;"
```

## 2. Bronze batch (CSV particionado) — opcional

Para popular o MinIO com partições **no mesmo layout** que o batch (`bronze/network_logs`, `cdr_customers`, `call_tests`, `towers`):

```bash
pip install pandas s3fs
python python_scripts/produce_bronze_batch.py
```

Isto gera dados **sintéticos** com o schema bronze (colunas e `day=YYYY-MM-DD/data.csv`); **não** corre a simulação de tempestade nem lê `Dados_Raw`. Para o cenário Leiria completo a partir de RAW, usar Flyte `ingest_pipeline_raw_to_bronze`. Não alimenta o Kafka.

## 3. Criar tabelas Iceberg (streaming)

No Trino UI (`http://localhost:8080`) ou via CLI:

1. `sql_scripts/setup_streaming_tables.sql` — instalação nova
2. `sql_scripts/migrate_streaming_dedup.sql` — se já tinha tabelas sem `ingest_batch_id` / checkpoint

## 4. Simular eventos de rede

No host (fora do Docker):

```bash
pip install -r python_scripts/requirements.txt
python python_scripts/producer_network_events.py
```

O broker exposto é `localhost:19092` (porta externa do Redpanda).

## 5. Validar leitura Kafka no Trino

```sql
SELECT * FROM kafka.default.network_events LIMIT 10;
```

## 6. Workflows Flyte (remoto)

| Workflow | Ficheiro | Função |
|----------|----------|--------|
| **`jdpt_streaming_full_sync`** | `streaming_full_sync.py` | **Kafka → bronze → silver → gold** (único pipeline incremental) |
| `jdpt_streaming_quality_check` | `avaliar_streaming.py` | QA duplicados e órfãos (opcional) |

| Camada | Task (raiz) | Lógica (`workflow_functions/streaming/`) |
|--------|-------------|------------------------------------------|
| Kafka → bronze | `streaming_kafka_to_bronze.py` | `kafka_to_bronze.py` |
| Bronze → silver | `workflows_incremental_streaming.py` | `bronze_to_silver.py` |
| Silver → gold | `streaming_silver_to_gold.py` | `silver_to_gold.py` |
| QA | `avaliar_streaming.py` | `quality.py` |

Exemplo (pipeline completo):

```bash
pyflyte register flyte-workflows/
pyflyte run --remote flyte-workflows/streaming_full_sync.py jdpt_streaming_full_sync
pyflyte run --remote flyte-workflows/avaliar_streaming.py jdpt_streaming_quality_check
```

## 7. Ligação ao domínio Leiria

- O **batch** continua a alimentar `network_logs`, CDR, torres e gold `network_quality_daily` / `churn_risk_daily`.
- O **streaming** simula medições de campo (`rsrp`, `sinr`, coordenadas na zona de Leiria) para monitorização contínua e testes de idempotência.

Para dashboards Superset, pode registar `iceberg.gold.network_events_hourly` como dataset adicional.

## Erros Flyte (`TypeError: bad argument type`)

Se o **2.º workflow** falhar com `TypeError: bad argument type for built-in operation` no `pyflyte-execute`, o Flyte **escondeu** o erro real do Trino. Ver os logs brutos do pod ou voltar a correr após `pyflyte register` com o código atualizado (as tasks re-levantam `RuntimeError` com a mensagem SQL).

**`MERGE_TARGET_ROW_MULTIPLE_MATCHES` no kafka→bronze:** o tópico Kafka tem o mesmo `event_id` várias vezes (producer ~10% duplicados). O workflow deduplica com `ROW_NUMBER` antes do MERGE. Se a bronze já tiver duplicados de runs antigas com `INSERT`, descomenta a limpeza em `migrate_streaming_dedup.sql`.

Causas frequentes do bronze→silver (passo 2 do `full_sync`):

1. Não correr `sql_scripts/setup_streaming_tables.sql` (falta `silver.network_events_clean` ou `streaming_checkpoints`).
2. Bronze vazio — producer ativo antes de `jdpt_streaming_full_sync` (o passo 1 do workflow ingere Kafka→bronze).
3. Coluna `ingest_batch_id` em falta na bronze — correr `migrate_streaming_dedup.sql`.

Ordem correta: **setup SQL** → **producer** → **`jdpt_streaming_full_sync`**.

## Portas e credenciais

| Serviço | Host (Flyte tasks) | Host (máquina local) |
|---------|-------------------|----------------------|
| Kafka/Redpanda | — | `localhost:19092` |
| Trino | `host.docker.internal:8080` | `localhost:8080` |
| MinIO | `host.docker.internal:9000` | `localhost:9000` |
| Loki | `host.docker.internal:3100` | `localhost:3100` |
| Grafana | — | `http://localhost:3000` |

## Logs no Grafana (Loki)

O **producer** (host) e as **tasks Flyte** enviam logs para Loki via [`workflow_functions/loki_logging.py`](../flyte-workflows/workflow_functions/loki_logging.py).

| Origem | `module` (label Loki) | URL Loki |
|--------|----------------------|----------|
| Producer (máquina local) | `producer_network_events` | `http://localhost:3100/loki/api/v1/push` |
| Flyte pods | `streaming_kafka_to_bronze`, `workflows_incremental_streaming`, `streaming_silver_to_gold`, … | `http://host.docker.internal:3100/loki/api/v1/push` |

### Setup

1. Subir stack: `docker compose up -d loki grafana`
2. Instalar deps do producer: `pip install -r python_scripts/requirements.txt`
3. Abrir Grafana: `http://localhost:3000`
4. **Connections → Data sources → Add Loki** → URL `http://loki:3100`
5. Correr producer: `python python_scripts/producer_network_events.py` (`--no-loki` desativa push)

### Explore — ver tráfego

Todos os logs streaming:

```logql
{application="tead", module=~"producer_network_events|streaming_kafka_to_bronze|workflows_incremental_streaming|streaming_silver_to_gold|avaliar_streaming"}
```

Eventos enviados ao Kafka (producer):

```logql
{application="tead", module="producer_network_events"} |= "event_sent"
```

### Auditoria de inserts (`table_insert`)

Cada MERGE/INSERT regista linhas com `table_insert` (função, tabela alvo, `new_rows`, `rows_inserted`, etc.):

```logql
{application="tead"} |= "table_insert"
```

Só quando terminou um passo (`phase=complete`):

```logql
{application="tead"} |= "table_insert" |= "phase=complete"
```

Por função:

```logql
{application="tead"} |= "table_insert" |= "function=ingest_kafka_to_bronze"
{application="tead"} |= "table_insert" |= "function=incremental_bronze_to_silver_network_events"
{application="tead"} |= "table_insert" |= "function=silver_to_gold_network_events_hourly"
```

### Dashboard — quando foi processado

**Logs** (recomendado — vê tabela, função e contagens):

```logql
{application="tead"} |= "table_insert" |= "phase=complete"
```

**Bar chart** — picos por minuto:

```logql
sum by (function) (
  count_over_time({application="tead"} |= "table_insert" |= "phase=complete"[1m])
)
```

Após alterar código Flyte, voltar a registar: `pyflyte register flyte-workflows/`.
