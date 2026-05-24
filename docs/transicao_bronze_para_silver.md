# Bronze → Silver

Resumo do que cada task `process_*_to_silver` faz aos CSV bronze e ao carregar `iceberg.silver.*`.

**Pré-requisito:** ingestão raw → bronze e validação bronze estão documentados em [Raw → Bronze](transicao_raw_para_bronze.md).

## Comum a todos os domínios (padrão do pipeline silver)

- **Leitura:** listagem S3 em `warehouse/bronze/<domínio>/`, concatenação de todos os `*.csv` do prefixo.
- **Normalização de nomes:** colunas em minúsculas, espaços → `_`, renomes explícitos por domínio (ex.: colunas com parênteses nos logs).
- **Limpeza numérica:** strings com vírgula decimal → ponto; remoção de lixo textual; `to_numeric`; **circuit breaker** se taxa de destruição > limiar (`silver_quarantine.NUMERIC_DESTROY_THRESHOLD`) → falha da task.
- **Datas / timestamps:** após o CSV bronze, `pd.to_datetime(..., errors="coerce", format="mixed")` onde aplicável — o pandas não pode assumir só `%Y-%m-%d %H:%M:%S` quando a string traz frações de segundo (ex.: `.132668310`); sem `format="mixed"` uma fatia grande de linhas virava NaT e disparava o circuit breaker de qualidade.
- **Quarentena:** linhas com campos destruídos na limpeza → insert em tabelas `*_quarantine_raw` / `*_quarantine_audit` (Trino) e remoção do lote principal.
- **Carga silver (ACID):** transacção Trino única — `START TRANSACTION` → `DELETE FROM iceberg.silver.<tabela> WHERE TRUE` → `INSERT INTO ... SELECT ... FROM hive.staging.temp_<tabela>` → `COMMIT`. Falhas disparam `ROLLBACK` e o snapshot anterior fica preservado (`workflow_functions/trino_acid.py`). `TRUNCATE` foi removido por não respeitar transacções no Iceberg Trino.
- **Staging:** Parquet em MinIO via `hive.staging.temp_<tabela>` (external_location) — inalterado.
- **`silver_row_id` monotónico:** `ROW_NUMBER` sobre chave estável; com DELETE prévio na mesma transacção, `MAX(silver_row_id)` devolve NULL → COALESCE dá 0.
- **Substituição:** silver é **full replace** por execução (não merge incremental por partição na app).

## `process_logs_to_silver` (`bronze/network_logs/` → `silver.network_logs`)

- Renome de colunas de negócio (ex.: `network provi.` → `network_provider`, `downlink(mbps)` → `downlink_mbps`).
- Tipos fortes para RSRP, RSRQ, SINR, PCI, downlink/uplink, velocidade, lat/lon.
- **`transform_network_logs_silver_features`:** `network_type` → colunas booleanas `nt_ohe_*`; timestamp como `timestamp_log`.
- Colunas duplicadas no bronze: mantém-se a **última** ocorrência (útil se existir coluna injetada duplicada).
- **`timestamp`:** antes da carga, normalização com `format="mixed"` (compatível com timestamps bronze com subsegundos).

## `process_cdr_to_silver` (`bronze/cdr_customers/` → `silver.cdr_customers`)

- Ordem fixa de colunas bronze; limpeza de numéricos e de `churn` (boolean com mapeamento de strings).
- Deduplicação por `drop_duplicates` após quarentena.
- Export Parquet para staging e carga Trino/Iceberg.

## `process_call_tests_to_silver` (`bronze/call_tests/` → `silver.call_tests`)

- Leitura CSV com **`decimal=','`** (formato europeu).
- Limpeza numérica (MOS, duração, setup, distância, sinal, etc.).
- **`date_of_test`:** parsing com `errors="coerce"` e **`format="mixed"`** para não perder linhas com frações de segundo no CSV bronze.
- **`transform_call_tests_silver_features`:** `call_test_result` → **`result` boolean** (ex.: `DROP`/`FAIL` → `false`); tecnologia → `tech_ohe_*`; renomes para `duration_s`, `setup_time_s`, etc.

## `process_towers_to_silver` (`bronze/towers/` → `silver.towers`)

- Separador **vírgula** nos CSV bronze de torres.
- Renome de aliases (ex.: `range` → `range_m`, `Snapshot_Date` → `snapshot_date`).
- **`transform_towers_silver_features`:** `status` → boolean; coluna `radio` → one-hot `radio_ohe_*`; remoção da coluna `radio` textual.

## O que deixa de existir na silver (em relação ao bronze)

- Strings “sujas” nos numéricos (substituídas por `NULL`/boolean após regra).
- Colunas brutas de resultado/tecnologia/radio nos formatos originais (passam a boolean normalizado + nomes canónicos).
