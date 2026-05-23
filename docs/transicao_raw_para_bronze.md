# Raw → Bronze

Resumo da simulação **Leiria / tempestade (jan. 2026)**: leitura dos CSV em `Dados_Raw`, transformação e escrita de partições **bronze** no MinIO/S3. A lógica de negócio está em `flyte-workflows/workflow_functions/bronze_storm_simulation.py`.

**Bronze sintético (schema apenas):** `python python_scripts/produce_bronze_batch.py` — gera partições `bronze/*/day=*/data.csv` com as colunas esperadas pelo batch silver, **sem** ler RAW nem simular tempestade.

**Bronze completo (tempestade Leiria a partir de RAW):** Flyte `ingest_pipeline_raw_to_bronze` ou `run_bronze_storm_simulation` em `bronze_storm_simulation.py` (requer `python setup_raw_data.py` antes).

## Origem (camada raw no bucket `warehouse`)

| Ficheiro | Uso |
|----------|-----|
| `s3://warehouse/Dados_Raw/opencellid_pt.csv` | Torres OpenCellID (filtro geográfico Leiria) |
| `s3://warehouse/Dados_Raw/Cellular Network Handover Prediction Dataset.csv` | Logs de rede (handover) |
| `s3://warehouse/Dados_Raw/CDR-Call-Details.csv` | CDR / clientes |
| `s3://warehouse/Dados_Raw/Call Tests Measurements for MOS prediction.csv` | Call tests |

Credenciais e endpoint S3 vêm de `get_storage_options()` (`TASK_ENV` / MinIO no pod Flyte).

## Flyte

- **Workflow:** `ingestion_workflow` em `flyte-workflows/raw_bronze_workflow.py`.
- **Task única de ingestão:** `ingest_pipeline_raw_to_bronze` (`ingest_pipeline_raw_to_bronze.py`) — **um pod** corre `run_bronze_storm_simulation(...)`.
- **A seguir na mesma DAG:** `avaliar_bronze` valida as partições bronze (ver `avaliar_bronze.py`).

## Ordem das fases (monólito)

Tudo corre **no mesmo processo**. Entre fases não há ficheiros de handoff em S3 (ex.: já não se usa `bronze/_storm_meta/`): passam arrays/`DataFrame`/listas em memória.

### 1. Torres (`_towers_phase`)

- Recorte Leiria (lat/lon), janela de simulação **2026-01-20** a **2026-02-04**.
- **Leste:** torres com `lon > -8.80`; ~80% dessas marcas como destruídas após **2026-01-28** (`Status` = `DOWN`); restantes `ACTIVE`.
- Coordenadas únicas das torres **DOWN** ficam em `down_arr` para a fase de logs.
- Escrita: `s3://warehouse/bronze/towers/day=<YYYY-MM-DD>/data.csv` (separador **vírgula**), colunas incl. `Snapshot_Date`, `Status`, `_ingested_at`.

### 2. Logs + preparação de call tests (`_logs_phase`)

- Lê os três datasets (logs, CDR, call tests); **offset** de lat/lon para Leiria.
- **Tempo:** alinha o mínimo dos timestamps ao **2026-01-20**; replica a série em blocos de +3 dias até cobrir a janela; corta após **2026-02-04**.
- **Identidades:** `np.random.seed(42)` — 2500 `Phone_Number` partilhados entre logs e call tests (amostra dos telefones do CDR).
- **Call tests — datas:** comprime para 16 dias com deltas aleatórios em segundos; a coluna `Date Of Test` é forçada a **`datetime64[ns]`** após o primeiro preenchimento (evita conflito `us`/`ns` ao mover datas com `uniform` na janela da tempestade).
- **Tempestade 2026-01-28 … 2026-01-30:**  
  - **Leste** (`lon > -8.80`): posiciona logs às coords das torres DOWN + jitter; piora RSRP/RSRQ/SINR, tecnologia fallback UMTS/GSM, velocidade 0.  
  - **Oeste** (`lon <= -8.80`): degrada downlink/uplink Mbps.  
- **Clientes “Leste”:** telefones únicos dos logs Leste em tempestade — lista passada em memória à fase CDR.
- **Call tests:** move parte dos testes (clientes Leste, fora da janela) para dentro da tempestade; marca **DROP**, MOS baixo, métricas com **vírgula** decimal (formato europeu); `_ingested_at`.
- **Logs:** remove `DeviceID` se existir; particiona por dia de `Timestamp` → `s3://warehouse/bronze/network_logs/day=<YYYY-MM-DD>/data.csv` (**`;`**).

### 3. Call tests bronze (`_call_tests_phase`)

- Particiona o `DataFrame` de call tests já preparado (memória) por `Date Of Test` → `s3://warehouse/bronze/call_tests/day=<YYYY-MM-DD>/data.csv` (**`;`**).

### 4. CDR bronze (`_cdr_phase`)

- Relê o CDR raw; incrementa `CustServ Calls` e ajusta `Churn` para linhas cujo `Phone Number` está em **clientes Leste** (lista em memória).
- Replica o mesmo snapshot CDR (com `_ingested_at`) para **cada dia** 2026-01-20 … 2026-02-04: `s3://warehouse/bronze/cdr_customers/day=<YYYY-MM-DD>/data.csv` (**`;`**).

## Destino bronze (padrão de paths)

- `warehouse/bronze/towers/day=<data>/data.csv`
- `warehouse/bronze/network_logs/day=<data>/data.csv`
- `warehouse/bronze/call_tests/day=<data>/data.csv`
- `warehouse/bronze/cdr_customers/day=<data>/data.csv`

## O que o bronze preserva de propósito (“sujidade”)

- RSRP/RSRQ/MOS/durações como **strings** com unidades ou vírgulas, para as tasks silver aplicarem limpeza e quarentena.

## Próximo passo na medalha

- [Bronze → Silver](transicao_bronze_para_silver.md) — leitura destes CSV, normalização e carga `iceberg.silver.*` (incl. `format="mixed"` em datas/timestamps após round-trip CSV).
