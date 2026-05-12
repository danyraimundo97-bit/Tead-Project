# Silver → Gold

Resumo do que as tasks `build_gold_*` fazem sobre `iceberg.silver.*` e o que muda na **granularidade** e nos **nomes** das colunas de produto.

## Pré-requisito no workflow

- **`jdpt_lakehouse_pipeline`** (`flyte-workflows/workflow.py`): `ensure_silver_layer_environment` → quatro tasks `process_*_to_silver` em paralelo (CDR, logs, call tests, torres) → **`avaliar_silver`** (validações de qualidade sobre silver; não altera dados; bloqueia avanço para gold se falhar) → `ensure_gold_layer_environment` → `build_gold_churn_risk` e `build_gold_network_quality` (gold em paralelo após o ambiente gold).

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
- **Carga:** `TRUNCATE` gold + `INSERT` único; validação pós-insert de colunas de antena (não todas nulas).

## `build_gold_churn_risk` → `iceberg.gold.churn_risk_daily`

- **Grain:** de **um cliente CDR** (silver) para **uma linha por (cliente × `Data_Referencia`)** — uma linha por telefone **por dia** em que exista atividade em call tests **ou** network logs (união de datas distintas).
- **Inserção:** em **ciclos por dia** (SQL por data) para limitar memória no Trino; `gold_row_id` continua monotónico no acumulado da tabela.
- **Campos principais:** `Telefone` = CDR; `Receita_Em_Risco` = soma de cargas diárias CDR; `Tempo_Subscrito`, `Total_Chamadas_Suporte`, `Desistencia` do CDR; `Total_Drops` / `Qualidade_Audio_MOS` agregados dos **call tests desse dia**; `Afetado_Tempestade` = se nesse dia existiu log com **longitude > -8.80** (regra “Leste” na silver).
- **Carga:** `TRUNCATE` + múltiplos `INSERT`; validação de colunas core não todas NULL.

## O que a gold não contém

- Não replica linha a linha silver; não substitui quarentena; não recalcula limpezas bronze — assume silver já consistente com as regras das tasks silver (incluindo datas/timestamps parseados corretamente a partir dos CSV bronze).
