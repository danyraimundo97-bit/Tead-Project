# Otimizações Iceberg (gold + silver batch)

Resumo das decisões de engenharia que cobrem **Particionamento**, **Pruning**, **Materialização vs Query-on-Read** e **ACID** no enunciado TEAD.

Ver também:

- [transicao_bronze_para_silver.md](transicao_bronze_para_silver.md) — carga silver ACID
- [transicao_silver_para_gold.md](transicao_silver_para_gold.md) — gold, particionamento, ACID

## 1. Particionamento e pruning

| Tabela | `partitioning` | Ficheiro |
|--------|----------------|----------|
| `gold.network_quality_daily` | `ARRAY['day("Data_Hora")']` | [`ensure_pipeline_layers.py`](../flyte-workflows/ensure_pipeline_layers.py) |
| `gold.churn_risk_daily` | `ARRAY['"Data_Referencia"']` | idem |

**Hidden partitioning:** filtros temporais (`WHERE "Data_Hora" >= ...`, `WHERE "Data_Referencia" = ...`) acionam partition pruning sem coluna `day` extra na query.

**Aplicar em ambiente existente:**

```bash
pyflyte run --remote flyte-workflows/clean_gold_layer.py reset_gold_workflow
pyflyte run --remote flyte-workflows/workflow.py jdpt_lakehouse_pipeline
```

## 2. Materialização vs query-on-read

| Artefacto | Modo | Justificação |
|-----------|------|--------------|
| `gold.network_quality_daily` | Materializado (ACID DELETE+INSERT) | Join Haversine + agregações pesadas |
| `gold.churn_risk_daily` | Materializado (ACID, N INSERTs/dia numa transacção) | Loop por dia; volume moderado |
| `gold.vw_storm_impact_executive` | Query-on-read (VIEW) | Cruza A+B sem duplicar; sempre fresca após batch |

Script: [`sql_scripts/create_semantic_views.sql`](../sql_scripts/create_semantic_views.sql)

```bash
# Trino UI ou CLI, após pipeline gold
# Executar create_semantic_views.sql
```

## 3. ACID na camada analítica (batch)

Helper: [`workflow_functions/trino_acid.py`](../flyte-workflows/workflow_functions/trino_acid.py)

| Task | Ficheiro |
|------|----------|
| `process_cdr_to_silver` | [`process_cdr_to_silver.py`](../flyte-workflows/process_cdr_to_silver.py) |
| `process_logs_to_silver` | [`process_logs_to_silver.py`](../flyte-workflows/process_logs_to_silver.py) |
| `process_call_tests_to_silver` | [`process_call_tests_to_silver.py`](../flyte-workflows/process_call_tests_to_silver.py) |
| `process_towers_to_silver` | [`process_towers_to_silver.py`](../flyte-workflows/process_towers_to_silver.py) |
| `build_gold_network_quality` | [`build_gold_network_quality.py`](../flyte-workflows/build_gold_network_quality.py) |
| `build_gold_churn_risk` | [`build_gold_churn_risk.py`](../flyte-workflows/build_gold_churn_risk.py) |

Padrão por task:

```
START TRANSACTION
  DELETE FROM <tabela> WHERE TRUE
  INSERT INTO <tabela> SELECT ...
COMMIT   -- ROLLBACK em falha
```

### Garantias

- **Atomicity:** ROLLBACK preserva snapshot anterior
- **Isolation:** snapshot isolation Iceberg — leitores não vêem tabela vazia durante pipeline
- **Durability:** manifest + Parquet no MinIO

### Verificação

```sql
SELECT committed_at, operation, summary
FROM iceberg.gold."network_quality_daily$snapshots"
ORDER BY committed_at DESC LIMIT 5;

EXPLAIN SELECT * FROM iceberg.gold.churn_risk_daily
WHERE "Data_Referencia" = DATE '2026-01-29';
```

## Categorias UC TEAD

| Categoria | Estado |
|-----------|--------|
| Particionamento | Implementado (gold) |
| Pruning | Consequência do particionamento |
| Materialização vs query-on-read | Implementado (marts + VIEW) |
| ACID batch | Implementado (6 tasks) |
| Redução shuffle / skew | Parcial (`_TOWERS_DEDUP_SQL` em network quality) |
| Backfills / parametrização temporal | Futuro (streaming cobre incremental via MERGE) |
