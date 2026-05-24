# Silver → Gold

Resumo do que as tasks `build_gold_*` fazem sobre `iceberg.silver.*` e o que muda na **granularidade** e nos **nomes** das colunas de produto.

## Pré-requisito no workflow

- **`jdpt_lakehouse_pipeline`** (`flyte-workflows/workflow.py`): execução **manual** (sem agendamento) que reconstrói o snapshot estático silver→gold. Fluxo: `ensure_silver_layer_environment` → quatro tasks `process_*_to_silver` em paralelo (CDR, logs, call tests, torres) → **`avaliar_silver`** (validações de qualidade sobre silver; não altera dados; bloqueia avanço para gold se falhar) → `ensure_gold_layer_environment` → `build_gold_churn_risk` e `build_gold_network_quality` (gold em paralelo após o ambiente gold).

## `build_gold_network_quality` → `iceberg.gold.network_quality_daily`

- **Grain:** de **um log por linha** (silver) para **uma linha por (dia de calendário do log × torre atribuída)**.
- **Torres usadas:** CTE `towers_for_geo` — **uma linha por (mcc, net, area, cell, unit)** com o **snapshot mais recente** em `snapshot_date` (evita duplicar a mesma antena física em vários `silver_row_id`).
- **Atribuição de torre:** para cada log com **lat/lon não nulos**, calcula-se distância Haversine a todas as torres em `towers_for_geo`; fica a torre com **menor distância** (empate resolvido por `tower_silver_row_id`).
- **Logs sem coordenadas:** **não entram** no cálculo (não aparecem na gold).
- **Agregações por dia/torre:** médias de distância, RSRP/RSRQ/SINR/downlink com **filtros de plausibilidade** (ex.: RSRP só entre -140 e -44 dBm; SINR/downlink com limites) para não deixar outliers destruir médias.
- **Dimensão temporal na gold:** `Data_Hora` = início do dia (`CAST(log_day AS TIMESTAMP)`), não o instante do log.
- **Zona_Leiria:** quadrante derivado das **coordenadas da torre** (não da posição média dos terminais).
- **Tecnologia_Rede:** tecnologia da **torre** (`radio_ohe_*` OpenCellId), não do `nt_ohe_*` do log.
- **Estado_Antena:** `status` boolean da **torre** na linha deduplicada (não inferido a partir da qualidade do sinal do log).
- **Call tests:** join por **`phone_number` + dia** com `tests_by_day` (`max_by(result, date_of_test)`); contagens **`Telefones_Sucesso` / `Telefones_Falha` / `Telefones_Sem_Teste`** (distinct por telefone no grupo).
- **Colunas extra de coordenadas:** `Torre_Latitude` / `Torre_Longitude` espelham a torre (além de `Latitude_Ocorrencia` / `Longitude_Ocorrencia`).
- **Carga:** transacção ACID (`DELETE WHERE TRUE` + `INSERT` + `COMMIT` via `trino_acid.py`); validação pós-insert de colunas de antena (não todas nulas).

## `build_gold_churn_risk` → `iceberg.gold.churn_risk_daily`

- **Grain:** de **um cliente CDR** (silver) para **uma linha por (cliente × `Data_Referencia`)** — uma linha por telefone **por dia** em que exista atividade em call tests **ou** network logs (união de datas distintas).
- **Inserção:** em **ciclos por dia** (SQL por data) dentro de **uma única transacção** ACID; `gold_row_id` continua monotónico no acumulado da tabela.
- **Campos principais:** `Telefone` = CDR; `Receita_Em_Risco` = soma de cargas diárias CDR; `Tempo_Subscrito`, `Total_Chamadas_Suporte`, `Desistencia` do CDR; `Total_Drops` / `Qualidade_Audio_MOS` agregados dos **call tests desse dia**; `Afetado_Tempestade` = se nesse dia existiu log com **longitude > -8.80** (regra “Leste” na silver).
- **Carga:** transacção ACID com múltiplos `INSERT` (1 por dia) + `COMMIT`; validação de colunas core não todas NULL.

## Particionamento Iceberg (gold)

| Tabela | `partitioning` | Coluna fonte |
|--------|----------------|--------------|
| `gold.churn_risk_daily` | `ARRAY['"Data_Referencia"']` | identidade (já é DATE) |
| `gold.network_quality_daily` | `ARRAY['day("Data_Hora")']` | `day(TIMESTAMP)` |

Definido em [`ensure_pipeline_layers.py`](../flyte-workflows/ensure_pipeline_layers.py). **Hidden partitioning:** filtros temporais como `WHERE "Data_Hora" >= DATE '...'` ou `WHERE "Data_Referencia" = DATE '...'` acionam partition pruning sem coluna `day` extra na query.

Re-criação obrigatória após alterar partitioning: `reset_gold_workflow` → `jdpt_lakehouse_pipeline`.

## Transacções ACID por task

Cada `build_gold_*` e `process_*_to_silver` substitui a tabela alvo numa transacção Trino:

```
START TRANSACTION
  DELETE FROM iceberg.<schema>.<tabela> WHERE TRUE
  INSERT INTO iceberg.<schema>.<tabela> SELECT ...
COMMIT  -- ou ROLLBACK em except
```

- Helper: [`workflow_functions/trino_acid.py`](../flyte-workflows/workflow_functions/trino_acid.py)
- `TRUNCATE` (anterior) auto-commitava → substituído por `DELETE WHERE TRUE`
- `autocommit = False` na conexão Trino
- Falha a meio → ROLLBACK → consumidores Superset/Grafana mantêm snapshot anterior (snapshot isolation Iceberg)
- `build_gold_churn_risk`: loop de N INSERTs (1 por dia) dentro de **uma única transacção**

## O que a gold não contém

- Não replica linha a linha silver; não substitui quarentena; não recalcula limpezas bronze — assume silver já consistente com as regras das tasks silver (incluindo datas/timestamps parseados corretamente a partir dos CSV bronze).
