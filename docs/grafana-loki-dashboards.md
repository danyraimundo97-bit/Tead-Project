# Grafana / Loki — queries TEAD (copiar para Explore / Dashboard)

**Datasource:** Loki  
**Selector base:** `{application="tead"}`  
**Variável Grafana (opcional):** `interval` = `5m` | `15m` | `1h`

Formato real da linha de log:

```text
INFO [streaming_kafka_to_bronze] table_insert function=ingest_kafka_to_bronze phase=complete kafka_source_rows=120 new_rows=15 rows_inserted=15 ...
INFO [producer_network_events] event_sent event_id=EVT_a1b2 dup=False topic=network_events ...
```

---

## Dashboard 1 — Kafka: publicados vs processados

### Painel A — Time series — eventos publicados (/min)

**Query A1 — total (inclui dup simulado):**

```logql
sum(rate({application="tead", module="producer_network_events"} |= "event_sent" [1m]))
```

**Query A2 — só envios únicos (recomendado):**

```logql
sum(rate({application="tead", module="producer_network_events"} |= "event_sent" |= "dup=False" [1m]))
```

**Query A3 — duplicados simulados:**

```logql
sum(rate({application="tead", module="producer_network_events"} |= "event_sent" |= "dup=True" [1m]))
```

### Painel B — Stat / Time series — processados na ingestão bronze

**B1 — nº de runs de ingestão concluídos** (1 log = 1 execução Flyte):

```logql
sum(count_over_time({application="tead", module="streaming_kafka_to_bronze"} |= "table_insert" |= "function=ingest_kafka_to_bronze" |= "phase=complete" [$interval]))
```

**B2 — linhas na fonte Kafka (último valor no intervalo):**

```logql
max(max_over_time({application="tead", module="streaming_kafka_to_bronze"} |= "table_insert" |= "ingest_kafka_to_bronze" |= "phase=complete" | regexp `kafka_source_rows=(?P<v>\d+)` | unwrap v [$interval]))
```

**B3 — linhas novas inseridas neste run:**

```logql
max(max_over_time({application="tead", module="streaming_kafka_to_bronze"} |= "table_insert" |= "ingest_kafka_to_bronze" |= "phase=complete" | regexp `rows_inserted=(?P<v>\d+)` | unwrap v [$interval]))
```

**B4 — eventos novos (ainda não estavam na bronze):**

```logql
max(max_over_time({application="tead", module="streaming_kafka_to_bronze"} |= "table_insert" |= "ingest_kafka_to_bronze" |= "phase=complete" | regexp `new_rows=(?P<v>\d+)` | unwrap v [$interval]))
```

### Painel C — Time series — silver processada (bronze → silver)

**C1 — runs concluídos:**

```logql
sum(count_over_time({application="tead", module="streaming_bronze_to_silver"} |= "table_insert" |= "function=incremental_bronze_to_silver_network_events" |= "phase=complete" [$interval]))
```

**C2 — linhas inseridas (estimativa do log):**

```logql
max(max_over_time({application="tead", module="streaming_bronze_to_silver"} |= "table_insert" |= "incremental_bronze_to_silver_network_events" |= "phase=complete" | regexp `rows_inserted=(?P<v>\d+)` | unwrap v [$interval]))
```

### Painel D — Logs (comparar manualmente)

**Publicados:**

```logql
{application="tead", module="producer_network_events"} |= "event_sent"
```

**Ingestão bronze (detalhe):**

```logql
{application="tead", module="streaming_kafka_to_bronze"} |= "table_insert" |= "function=ingest_kafka_to_bronze"
```

---

## Dashboard 2 — `table_insert` (auditoria streaming)

### Painel Logs — todas as cargas

```logql
{application="tead"} |= "table_insert"
```

### Por etapa

**Kafka → bronze:**

```logql
{application="tead"} |= "table_insert" |= "function=ingest_kafka_to_bronze"
```

**Bronze → silver:**

```logql
{application="tead"} |= "table_insert" |= "function=incremental_bronze_to_silver_network_events"
```

**Silver → gold horária:**

```logql
{application="tead"} |= "table_insert" |= "function=silver_to_gold_network_events_hourly"
```

### Por tabela Iceberg

```logql
{application="tead"} |= "table_insert" |= "target_table=iceberg.bronze.network_events_raw"
```

```logql
{application="tead"} |= "table_insert" |= "target_table=iceberg.silver.network_events_clean"
```

```logql
{application="tead"} |= "table_insert" |= "target_table=iceberg.gold.network_events_hourly"
```

### Time series — conclusões por módulo Flyte

```logql
sum by (module) (count_over_time({application="tead"} |= "table_insert" |= "phase=complete" [$interval]))
```

### Gold horária — buckets agregados inseridos

```logql
max(max_over_time({application="tead", module="streaming_silver_to_gold"} |= "table_insert" |= "silver_to_gold_network_events_hourly" |= "phase=complete" | regexp `new_rows=(?P<v>\d+)` | unwrap v [$interval]))
```

### Fases intermédias (delete antes do insert gold)

```logql
{application="tead", module="streaming_silver_to_gold"} |= "table_insert" |= "phase=after_delete"
```

```logql
{application="tead", module="streaming_silver_to_gold"} |= "table_insert" |= "phase=before_delete"
```

---

## Dashboard 3 — Modelos ML

> **Hoje:** sem logs de treino no Loki. MLflow planeado; usar queries abaixo quando existir `model_train` no código.

### Placeholder (vazio até integrar)

```logql
{application="tead"} |= "model_train"
```

### Futuro — treinos concluídos

```logql
{application="tead", module="ml_churn_training"} |= "model_train" |= "phase=complete"
```

### Futuro — AUC (métrica numérica)

```logql
max(max_over_time({application="tead"} |= "model_train" |= "phase=complete" | regexp `auc=(?P<v>[\d.]+)` | unwrap v [$interval]))
```

### Não confundir com coluna CSV `devicemodel`

```logql
{application="tead", module="process_logs_to_silver"} |= "Prepared"
```

---

## Dashboard 4 — Processamentos completos

### Streaming — mensagem de sucesso da task

```logql
{application="tead", module="streaming_kafka_to_bronze"} |= "Kafka → bronze"
```

```logql
{application="tead", module="streaming_bronze_to_silver"} |= "Bronze → silver"
```

```logql
{application="tead", module="streaming_silver_to_gold"} |= "Silver → gold"
```

### Streaming — contagem de etapas `phase=complete`

```logql
sum(count_over_time({application="tead", module="streaming_kafka_to_bronze"} |= "table_insert" |= "phase=complete" [$interval]))
```

```logql
sum(count_over_time({application="tead", module="streaming_bronze_to_silver"} |= "table_insert" |= "phase=complete" [$interval]))
```

```logql
sum(count_over_time({application="tead", module="streaming_silver_to_gold"} |= "table_insert" |= "phase=complete" [$interval]))
```

### Batch — Gold concluído

```logql
{application="tead", module="build_gold_network_quality"} |= "full batch completed"
```

```logql
{application="tead", module="build_gold_churn_risk"} |= "full batch completed"
```

### Batch — Silver concluído

```logql
{application="tead", module="process_cdr_to_silver"} |= "finished"
```

```logql
{application="tead", module="process_logs_to_silver"} |= "finished"
```

```logql
{application="tead", module="process_call_tests_to_silver"} |= "finished"
```

```logql
{application="tead", module="process_towers_to_silver"} |= "finished"
```

**Todos os silver batch:**

```logql
{application="tead", module=~"process_(cdr|logs|call_tests|towers)_to_silver"} |= "finished"
```

### Batch — commit ACID

```logql
{application="tead"} |= "ACID commit" |= "replaced atomically"
```

### QA streaming — OK

```logql
{application="tead", module="avaliar_streaming"} |= "Streaming QA: OK"
```

### QA streaming — alerta

```logql
{application="tead", module="avaliar_streaming"} |= "Streaming QA: ALERTA"
```

### Producer — arranque / paragem

```logql
{application="tead", module="producer_network_events"} |= "producer_started"
```

```logql
{application="tead", module="producer_network_events"} |= "producer_stopped"
```

---

## Dashboard 5 — Falhas e rollbacks

```logql
{application="tead"} |= "TASK FAILED"
```

```logql
{application="tead"} |= "falhou"
```

```logql
{application="tead"} |= "ACID rollback"
```

```logql
{application="tead"} |= "ERRO CRÍTICO"
```

```logql
{application="tead"} |~ "(?i)(TASK FAILED|falhou|ACID rollback|ERRO CRÍTICO)"
```

---

## Configuração Grafana

1. **Dashboard settings → Variables**
   - Name: `interval`
   - Type: Interval
   - Values: `1m,5m,15m,30m,1h`

2. **Painel Time series** — usar queries com `rate` ou `count_over_time [...]` e legenda clara.

3. **Painel Stat** — queries `max(max_over_time(...))` ou `sum(count_over_time(...))`.

4. **Painel Logs** — queries `{application="tead"} |= "..."` sem agregação.

5. Se `unwrap` não devolver dados, confirmar em Explore que a linha contém o campo (ex. `rows_inserted=15`) e que o intervalo de tempo cobre o último run Flyte.

---

## Pré-requisitos

```bash
# Producer com Loki
python python_scripts/producer_network_events.py

# Streaming (gera table_insert)
pyflyte run --remote flyte-workflows/streaming_full_sync.py jdpt_streaming_full_sync
```

`LOKI_URL` nas tasks: `http://host.docker.internal:3100/loki/api/v1/push` (ver `loki_logging.py`).
