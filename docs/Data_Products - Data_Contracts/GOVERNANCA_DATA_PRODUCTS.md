# Governança dos Data Products — checklist e cobertura

Documento de referência para o relatório TEAD: como os **Data Products** e **Data Contracts** do projeto cobrem as dimensões habituais de governança analítica (perguntas de negócio, métricas, consumidores, grão, contrato, evolução de schema).

**Localização no repositório:** `docs/Data_Products - Data_Contracts/`

| Papel | Ficheiros activos (raiz das pastas) | Versões históricas |
|-------|-------------------------------------|--------------------|
| Data Product A | [`Data_Products/Produto A - gold.network_quality_daily 1.txt`](Data_Products/Produto%20A%20-%20gold.network_quality_daily%201.txt) | [`v0/`](v0/), [`v1/`](v1/), [`v2/`](v2/) |
| Data Product B | [`Data_Products/Produto B - gold.churn_risk_daily 1.txt`](Data_Products/Produto%20B%20-%20gold.churn_risk_daily%201.txt) | idem |
| Data Contract A | [`Data_Contracts/Contrato A - gold.network_quality_daily 1.txt`](Data_Contracts/Contrato%20A%20-%20gold.network_quality_daily%201.txt) | idem |
| Data Contract B | [`Data_Contracts/Contrato B - gold.churn_risk_daily 1.txt`](Data_Contracts/Contrato%20B%20-%20gold.churn_risk_daily%201.txt) | idem |

**Versão activa em produção (schema):** **v1.0.0** (`v1/` e cópias na raiz).  
**Versão operacional (batch estático):** **v1.1.0** em [`v2/`](v2/) — ver [`COMPARACAO_v1_v2.md`](COMPARACAO_v1_v2.md).

---

## Resumo por dimensão

| Dimensão | Coberto? | Onde |
|----------|----------|------|
| Perguntas analíticas | Parcial (implícito) | Relatório [`docs/Relatório/main.tex`](../Relatório/main.tex); secção abaixo sugere enquadramento explícito |
| Métricas / KPIs | Sim | Contratos: `schema`, `semantics.definitions`; Gold: [`docs/Relatório/Ficheiro de construção da camada gold.md`](../Relatório/Ficheiro%20de%20construção%20da%20camada%20gold.md) |
| Consumidores (BI, ML, …) | Sim (BI + ML planeado) | Data Products: `consumers` |
| Grão e chaves | Sim | Produtos (cabeçalho) + Contratos: `interface.grain`, `primary_key` |
| Contrato de dados (schema, SLO) | Sim | Data Contracts |
| Schema evolution / versionamento | Sim | `v0/` → `v1/` → `v2/`, `change_management`, comparações |
| ACID (cargas atómicas) | Sim | `trino_acid.py` + 6 tasks batch (silver + gold) |
| Retries | Parcial | Streaming: Flyte + Trino; batch: sem retries explícitos na task |
| Particionamento / pruning | Sim | DDL Iceberg gold + contratos/produtos |
| Redução de shuffle / skew | Parcial | Dedup torres (Produto A); INSERTs por dia (Produto B) |
| Materialização vs query-on-read | Sim | Tabelas gold materializadas; VIEW executiva on-read |

---

## 1. Perguntas analíticas, métricas e consumidores

### Consumidores

Documentados em cada **Data Product** (`consumers`), com tipo e forma de acesso:

| Consumidor | Produto | Tipo | Acesso | Estado |
|------------|---------|------|--------|--------|
| `bi.infrastructure_map` | A | BI | Trino SQL | activo |
| `superset.network_quality_dashboard` | A | BI | Trino SQL | activo |
| `bi.churn_executive_dashboard` | B | BI | Trino SQL | activo |
| `superset.churn_risk_dashboard` | B | BI | Trino SQL | activo |
| `ml.churn_prediction_model` | B | ML | Trino SQL (`feature_table`) | **planned** |

Não há consumidor **API REST** documentado; o serving é via **Trino** e views estáveis (`iceberg.gold.vw_storm_impact_executive`).

### Métricas

| Onde | Conteúdo |
|------|----------|
| **Contrato** | Colunas com tipo, nullable e descrição; regras de qualidade (ex.: RSRP ∈ [−140, −44]) |
| **`semantics.definitions`** | Fórmulas de negócio (ex.: `drop_rate_proxy`, `Receita_Em_Risco`, `Afetado_Tempestade`) |
| **Pipeline** | Implementação em `flyte-workflows/build_gold_network_quality.py` e `build_gold_churn_risk.py` |
| **Relatório / anexo Gold** | KPIs derivados e regras de agregação |

### Perguntas analíticas (enquadramento de negócio)

Não existe ainda um bloco YAML `analytical_questions` nos ficheiros de produto; o objectivo analítico está na **descrição do produto** e no **relatório**. Para referência no relatório, as perguntas que os produtos respondem são:

**Produto A — `gold.network_quality_daily`**

- Quais torres estão **DOWN** ou com sinal degradado por dia e zona de Leiria?
- Qual a **qualidade de rádio** média (RSRP, RSRQ, SINR, downlink) por torre, com filtros 3GPP?
- Quantos **testes de voz** falharam vs. sucederam por torre (proxy de drop rate)?
- Onde concentrar **intervenção de rede** pós-tempestade (mapa NOC)?

**Produto B — `gold.churn_risk_daily`**

- Quanto **receita diária** está em risco por cliente e por dia (`Receita_Em_Risco`)?
- Quais clientes estão **expostos à tempestade** (`Afetado_Tempestade`) e com frustração (drops, MOS baixo)?
- Quem **desistiu** (`Desistencia`) para análise de churn e treino de modelos?
- Como segmentar campanhas de **retenção** (Marketing) e priorizar contacto?

---

## 2. Grão e chaves

| Produto | Tabela Iceberg | Grão | Chave de negócio | Chave técnica |
|---------|----------------|------|------------------|---------------|
| A | `iceberg.gold.network_quality_daily` | 1 linha por **(torre × dia civil)** | `(ID_Antena_Conectada, Data_Hora)` | `gold_row_id` (surrogate) |
| B | `iceberg.gold.churn_risk_daily` | 1 linha por **(cliente × dia de referência)** | `(Telefone, Data_Referencia)` | `gold_row_id` (surrogate) |

Definição canónica nos contratos, secção `interface`:

- Produto A: `grain: "1 linha por (ID_Antena_Conectada, dia de calendário)"`
- Produto B: `grain: "1 linha por (Telefone, Data_Referencia)"`

`Data_Hora` no Produto A é o **início do dia** de calendário (snapshot da torre).

---

## 3. Contrato de dados (schema, SLAs/SLOs)

Cada **Data Product** aponta para um **Data Contract** via `references.contract_file`.

O contrato define:

| Secção | Conteúdo |
|--------|----------|
| `contract` | `id`, `version`, `status`, `catalog_table` |
| `interface` | grão, PK, notas |
| `schema` | colunas, tipos Trino/Iceberg, semântica por campo |
| `semantics.definitions` | métricas derivadas e regras de negócio |
| `quality.constraints` | unicidade, intervalos, nulls |
| `quality.slo` | frescura dos dados |

### SLO de frescura (evolução)

| Versão contrato | Frescura |
|-----------------|----------|
| **v1.0.0** | P95 ≤ 4 h após fim do dia civil (design batch noturno Flyte) |
| **v1.1.0** (`v2/`) | Snapshot disponível após conclusão bem-sucedida do pipeline batch (cenário académico, run manual) |

Detalhe: [`COMPARACAO_v1_v2.md`](COMPARACAO_v1_v2.md).

### Separação Product vs Contract

| Data Product | Data Contract |
|--------------|---------------|
| Quem consome, como se entrega (MinIO/Iceberg, Trino, partições) | O que se promete na interface (schema, qualidade, SLO) |
| Orquestração (Flyte, workflow, schedule) | Regras de evolução (`change_management`) |
| Linhagem (`lineage.sources`) | Constraints e semântica de colunas |

---

## 4. Estratégia de schema evolution e versionamento

### Linha temporal

```
v0.1.0 (draft conceptual, inglês, UC)
    → v1.0.0 (alinhamento Trino/Iceberg + pipelines Flyte)
    → v1.1.0 (minor operacional: SLO/schedule; schema inalterado)
```

| Documento | Conteúdo |
|-----------|----------|
| [`COMPARACAO_v0_v1.md`](COMPARACAO_v0_v1.md) | v0 → v1: renomeação de colunas, grão, semântica `Receita_Em_Risco`, ACID |
| [`COMPARACAO_v1_v2.md`](COMPARACAO_v1_v2.md) | v1.0.0 → v1.1.0: batch estático; interface de dados igual |

### Política futura (`change_management`)

Regra em ambos os contratos v1+:

| Alteração | Bump | Compatibilidade |
|-----------|------|-----------------|
| `add_nullable_column` | **minor** | backward |
| `rename_column` | **major** | breaking |

Em **v1.1.0** (`v2/`), acrescenta-se regra para alterações só de `production` / SLO (minor, sem breaking de schema).

Produto B: `late_data_policy` — latência até 3 dias; correcção via `acid_replace` (rebuild gold).

---

## 5. Engenharia de execução (ACID, retries, particionamento, shuffle, materialização)

Onde cada decisão é **aplicada** no repositório (batch lakehouse + streaming NOC). Documentação técnica detalhada: [`docs/otimizacoes_iceberg.md`](../otimizacoes_iceberg.md), [`docs/transicao_silver_para_gold.md`](../transicao_silver_para_gold.md), [`docs/transicao_bronze_para_silver.md`](../transicao_bronze_para_silver.md).

### Resumo

| Prática | Onde aplicamos | Ficheiro(s) principal(is) |
|---------|----------------|---------------------------|
| **ACID** (substituição atómica) | Silver (4 tabelas) + Gold (2 produtos) | [`flyte-workflows/workflow_functions/trino_acid.py`](../../flyte-workflows/workflow_functions/trino_acid.py) |
| **Retries** | Pipeline **streaming** (Flyte + Trino); producer Kafka | [`streaming/task_config.py`](../../flyte-workflows/workflow_functions/streaming/task_config.py), [`streaming_trino_client.py`](../../flyte-workflows/workflow_functions/streaming_trino_client.py) |
| **Particionamento Iceberg** | DDL gold (+ pruning em leitura) | [`ensure_pipeline_layers.py`](../../flyte-workflows/ensure_pipeline_layers.py) |
| **Redução de shuffle** | Pré-agregação / dedup antes de joins pesados | [`build_gold_network_quality.py`](../../flyte-workflows/build_gold_network_quality.py), [`build_gold_churn_risk.py`](../../flyte-workflows/build_gold_churn_risk.py) |
| **Mitigação de skew** | Dedup de torres; carga churn por dia | idem |
| **Materialização vs query-on-read** | Marts gold materializados; VIEW semântica | [`sql_scripts/create_semantic_views.sql`](../../sql_scripts/create_semantic_views.sql), [`otimizacoes_iceberg.md`](../otimizacoes_iceberg.md) |

Contratos e produtos **referenciam** ACID e partições em `delivery.update_mode: acid_replace` e `delivery.storage.partitions`; a **implementação** está nos pipelines Flyte/Trino.

---

### 5.1 ACID (atomicidade, isolamento, rollback)

**Padrão:** `START TRANSACTION` → `DELETE FROM <tabela> WHERE TRUE` → `INSERT` (um ou vários) → `COMMIT`; em falha, `ROLLBACK` preserva o snapshot Iceberg anterior.

| Camada | Task Flyte | Tabela | Helper |
|--------|------------|--------|--------|
| Silver | `process_cdr_to_silver` | `iceberg.silver.cdr_customers` | `replace_table_transaction` |
| Silver | `process_logs_to_silver` | `iceberg.silver.network_logs` | idem |
| Silver | `process_call_tests_to_silver` | `iceberg.silver.call_tests` | idem |
| Silver | `process_towers_to_silver` | `iceberg.silver.towers` | idem |
| Gold A | `build_gold_network_quality` | `iceberg.gold.network_quality_daily` | idem (1 INSERT) |
| Gold B | `build_gold_churn_risk` | `iceberg.gold.churn_risk_daily` | `replace_table_transaction_multi_insert` (N INSERTs, 1 transacção) |

**Garantias documentadas:** snapshot isolation Iceberg — leitores BI (Superset/Grafana) não vêem tabela vazia a meio do pipeline; falha → estado anterior intacto.

**Histórico:** v0 dos contratos previa `MERGE` por partição; v1+ alinha com **rebuild ACID** (`acid_replace`) — ver [`COMPARACAO_v0_v1.md`](COMPARACAO_v0_v1.md). Contrato B: `late_data_policy.correction_strategy` aponta para o mesmo padrão.

**Verificação:** snapshots Iceberg (`$snapshots`) — exemplos em [`otimizacoes_iceberg.md`](../otimizacoes_iceberg.md).

---

### 5.2 Retries

| Contexto | Mecanismo | Configuração |
|----------|-----------|--------------|
| **Streaming — Flyte** | Reexecução da task em falha | `STREAMING_TASK_KWARGS`: `retries: 3`, `timeout: 15min` — [`task_config.py`](../../flyte-workflows/workflow_functions/streaming/task_config.py) |
| **Streaming — Trino** | `execute_with_retry` / `fetch_one_with_retry` | Até 3 tentativas, backoff 2s / 5s / 10s — [`streaming_trino_client.py`](../../flyte-workflows/workflow_functions/streaming_trino_client.py) |
| **Streaming — Kafka** | Producer idempotente | `acks=all`, `retries=3`, `enable_idempotence=True` — [`docs/streaming.md`](../streaming.md), `python_scripts/producer_network_events.py` |
| **Batch lakehouse** | Sem `retries` explícitos nas `@task` silver/gold | Falha da task = re-run manual do workflow `jdpt_lakehouse_pipeline`; idempotência garantida pelo ACID (re-run seguro) |
| **Relatório** | Flyte como orquestrador com retries (capacidade da plataforma) | [`docs/Relatório/main.tex`](../Relatório/main.tex) (stack) |

Idempotência streaming: `PARTITION BY event_id` + MERGE em bronze/silver — [`kafka_to_bronze.py`](../../flyte-workflows/workflow_functions/streaming/kafka_to_bronze.py), [`bronze_to_silver.py`](../../flyte-workflows/workflow_functions/streaming/bronze_to_silver.py).

---

### 5.3 Particionamento e partition pruning

Definido no **DDL Iceberg** ao criar/recriar tabelas gold:

| Tabela (produto) | Expressão `partitioning` | Ficheiro |
|------------------|--------------------------|----------|
| `gold.network_quality_daily` (A) | `ARRAY['day("Data_Hora")']` | [`ensure_pipeline_layers.py`](../../flyte-workflows/ensure_pipeline_layers.py) (~L352) |
| `gold.churn_risk_daily` (B) | `ARRAY['"Data_Referencia"']` | idem (~L323) |

**Hidden partitioning:** filtros `WHERE "Data_Hora" >= ...` ou `WHERE "Data_Referencia" = DATE '...'` activam **partition pruning** sem coluna `day` explícita na query do consumidor.

**Contratos / produtos:** `delivery.storage.partitions` nos YAML (ex.: `day("Data_Hora")`, `Data_Referencia`).

**Operação:** após alterar partitioning, é necessário `reset_gold_workflow` + `jdpt_lakehouse_pipeline` — [`transicao_silver_para_gold.md`](../transicao_silver_para_gold.md).

**Pruning:** consequência directa do particionamento temporal; validável com `EXPLAIN` no Trino — [`otimizacoes_iceberg.md`](../otimizacoes_iceberg.md).

---

### 5.4 Redução de shuffle e mitigação de skew

No stack TEAD (Trino SQL, sem Spark), “shuffle” traduz-se sobretudo em **volume de dados movidos em joins e agregações**. Mitigações aplicadas:

| Técnica | Produto / camada | Implementação |
|---------|------------------|---------------|
| **Dedup antes do join** | Produto A (network quality) | CTE `_TOWERS_DEDUP_SQL`: `ROW_NUMBER() OVER (PARTITION BY mcc, net, area, cell, unit ...)` — uma torre física por célula; evita multiplicar linhas no join Haversine log↔torre — [`build_gold_network_quality.py`](../../flyte-workflows/build_gold_network_quality.py) (comentário L37–38) |
| **Agregação pré-join** | Produto A | `tests_by_day`, agregados por `(torre, dia)` antes de cruzar com a matriz torre×dia |
| **Carga por dia (chunking)** | Produto B (churn) | Um `INSERT` por `Data_Referencia` dentro da mesma transacção ACID — limita memória/pico por query no Trino — [`build_gold_churn_risk.py`](../../flyte-workflows/build_gold_churn_risk.py) (docstring L104–105) |
| **Filtros 3GPP nas médias** | Produto A | Reduz outliers que distorcem agregados (ex.: RSRP ∈ [−140, −44]) |
| **Streaming — dedup eventos** | Bronze/silver streaming | `PARTITION BY event_id` no MERGE |

**Estado (honesto):** [`otimizacoes_iceberg.md`](../otimizacoes_iceberg.md) classifica redução shuffle/skew como **parcial** — não há salting nem rebalanceamento explícito; o join Haversine log×todas as torres continua custoso por desenho académico.

---

### 5.5 Materialização vs query-on-read

| Artefacto | Modo | Onde | Motivo |
|-----------|------|------|--------|
| `iceberg.gold.network_quality_daily` | **Materializado** (batch ACID) | `build_gold_network_quality` | Join Haversine + agregações pesadas; consumo BI repetido |
| `iceberg.gold.churn_risk_daily` | **Materializado** (batch ACID, N dias/tx) | `build_gold_churn_risk` | Feature table para BI/ML; evita recomputar joins CDR×tests×logs |
| `iceberg.gold.vw_storm_impact_executive` | **Query-on-read** (VIEW) | [`sql_scripts/create_semantic_views.sql`](../../sql_scripts/create_semantic_views.sql) | Cruza Produtos A+B sem duplicar storage; lógica executiva sempre alinhada ao snapshot gold |
| Silver | Materializado (ACID full-replace) | `process_*_to_silver` | Camada limpa reutilizada por várias builds gold |
| Streaming gold (`streaming_silver_to_gold`) | Incremental MERGE | [`silver_to_gold.py`](../../flyte-workflows/workflow_functions/streaming/silver_to_gold.py) | Janela NOC; não substitui o batch dos data products contratados |

**Data products:** `delivery.update_mode: acid_replace` documenta materialização com substituição total por run batch.

**Relatório:** fluxo batch “materializa o estado final” Silver+Gold; streaming é janela deslizante para NOC — [`main.tex`](../Relatório/main.tex).

---

## 6. Lacunas e melhorias opcionais

1. **Perguntas analíticas** — adicionar secção `analytical_questions` nos YAML dos produtos (ou manter este MD como anexo ao relatório).
2. **API** — não modelada; consumo exclusivo SQL/BI/ML sobre Trino.
3. **ML** — consumidor `ml.churn_prediction_model` com `status: planned`; ver [`docs/mlflow_planned.md`](../mlflow_planned.md).

---

## 7. Referências cruzadas

| Tema | Ficheiro |
|------|----------|
| Construção Gold (grão, colunas, SLO, ACID) | [`docs/Relatório/Ficheiro de construção da camada gold.md`](../Relatório/Ficheiro%20de%20construção%20da%20camada%20gold.md) |
| Otimizações Iceberg (particionamento, ACID, materialização) | [`docs/otimizacoes_iceberg.md`](../otimizacoes_iceberg.md) |
| Relatório (narrativa CRISP-DM) | [`docs/Relatório/main.tex`](../Relatório/main.tex) |
| Transição Bronze → Silver (ACID silver) | [`docs/transicao_bronze_para_silver.md`](../transicao_bronze_para_silver.md) |
| Transição Silver → Gold | [`docs/transicao_silver_para_gold.md`](../transicao_silver_para_gold.md) |
| Streaming (retries, idempotência) | [`docs/streaming.md`](../streaming.md) |
| Pipelines batch | `flyte-workflows/build_gold_network_quality.py`, `build_gold_churn_risk.py`, `process_*_to_silver.py` |
| Helper ACID | `flyte-workflows/workflow_functions/trino_acid.py` |
