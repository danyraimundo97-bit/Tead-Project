# TEAD 2.0 — Stack local de dados

Lakehouse **batch** (CSV → Iceberg → Gold), **streaming** (Kafka/Redpanda) e consumo BI, com **Flyte** e **Docker Compose**.

Repositório: https://github.com/danyraimundo97-bit/Tead-Project

---

## 1. Subir a stack

### Pré-requisitos

- Docker Engine 20.10+ e Docker Compose v2
- ~8 GB RAM, ~10 GB disco
- Python 3.10+ (producer e scripts no host)

```bash
git clone https://github.com/danyraimundo97-bit/Tead-Project.git
cd Tead-Project
docker compose up -d --build
docker compose ps
docker compose exec trino trino --execute "SHOW CATALOGS;"
```

Catálogos esperados: `iceberg`, `hive`, `kafka`, `system`.

```bash
docker compose down        # mantém volumes
docker compose down -v     # reset total
```

### Portas

| Serviço | URL | Credenciais |
|---------|-----|-------------|
| MinIO API | http://localhost:9000 | `minioadmin` / `minioadmin` |
| MinIO Console | http://localhost:9001 | idem |
| Trino | http://localhost:8080 | sem auth |
| Redpanda (Kafka) | `localhost:19092` | — |
| Grafana | http://localhost:3000 | anónimo Admin |
| Loki | http://localhost:3100 | — |
| Superset | http://localhost:8088 | `admin` / `admin` |
| Flyte Console | http://localhost:30080 | sandbox |

Tasks Flyte no host acedem ao Compose via `host.docker.internal` (MinIO `9000`, Trino `8080`, Loki `3100`).

---

## 2. Flyte (sandbox)

**Windows (PowerShell):**

```powershell
$env:DOCKER_API_VERSION="1.44"
flytectl demo start --image cr.flyte.org/flyteorg/flyte-sandbox-bundled:latest
```

**Linux / macOS:** mesma imagem; em Linux pode ser necessário `--add-host host.docker.internal:host-gateway` no sandbox.

Registar workflows (sempre que alterar código em `flyte-workflows/`):

```bash
pyflyte register flyte-workflows/
```

Variáveis usadas pelas tasks (MinIO):

```text
MLFLOW_S3_ENDPOINT_URL=http://host.docker.internal:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
```

---

## 3. Ordem de execução dos workflows

### Caminho A — Batch (obrigatório para Produtos Gold A/B)

| Ordem | Workflow | Ficheiro | Quando |
|-------|----------|----------|--------|
| 1 | `ingestion_workflow` | `flyte-workflows/raw_bronze_workflow.py` | Bronze MinIO vazio: RAW → bronze (simulação Leiria) |
| 2 | **`jdpt_lakehouse_pipeline`** | `flyte-workflows/workflow.py` | Silver + Gold (snapshot completo) |

Se o bronze já estiver em `s3://warehouse/bronze/` (ex.: datasets preparados), **saltar o passo 1**.

```bash
pyflyte register flyte-workflows/

# Opcional — só se bronze estiver vazio
pyflyte run --remote flyte-workflows/raw_bronze_workflow.py ingestion_workflow

# Pipeline principal batch
pyflyte run --remote flyte-workflows/workflow.py jdpt_lakehouse_pipeline
```

**Reset gold** (após mudar particionamento Iceberg):

```bash
pyflyte run --remote flyte-workflows/clean_gold_layer.py reset_gold_workflow
pyflyte run --remote flyte-workflows/workflow.py jdpt_lakehouse_pipeline
```

### Caminho B — Streaming (independente do batch; NOC em tempo quase real)

| Ordem | Ação | Detalhe |
|-------|------|---------|
| 1 | DDL streaming | Executar `sql_scripts/setup_streaming_tables.sql` no Trino (`http://localhost:8080`) |
| 2 | Producer | `python python_scripts/producer_network_events.py` (broker `localhost:19092`) |
| 3 | **`jdpt_streaming_full_sync`** | `flyte-workflows/streaming_full_sync.py` |
| 4 | (opcional) `jdpt_streaming_quality_check` | `flyte-workflows/avaliar_streaming.py` |

```bash
pip install -r python_scripts/requirements.txt
python python_scripts/producer_network_events.py

pyflyte register flyte-workflows/
pyflyte run --remote flyte-workflows/streaming_full_sync.py jdpt_streaming_full_sync
pyflyte run --remote flyte-workflows/avaliar_streaming.py jdpt_streaming_quality_check
```

Validar Kafka no Trino:

```sql
SELECT * FROM kafka.default.network_events LIMIT 10;
```

Migração bronze antiga (sem `ingest_batch_id`): `sql_scripts/migrate_streaming_dedup.sql`.

---

## 4. Diagramas e figuras

### Fontes (Mermaid / BPMN)

| Conteúdo | Ficheiro |
|----------|----------|
| Tabelas batch (Mermaid) | `docs/diagrams/lakehouse-tables-batch.mmd` |
| Tabelas streaming (Mermaid) | `docs/diagrams/lakehouse-tables-streaming.mmd` |
| BPMN batch (editar em bpmn.io) | `docs/diagrams/batch-jdpt_lakehouse_pipeline.bpmn` |
| BPMN streaming | `docs/diagrams/streaming-jdpt_streaming_full_sync.bpmn` |

Visualizar `.mmd`: [mermaid.live](https://mermaid.live) (um ficheiro de cada vez).

### Figuras para o relatório (PNG/PDF)

Pasta: `docs/Relatório/figuras/` — exportar BPMN e diagramas a partir do bpmn.io / Mermaid CLI e usar em `docs/Relatório/main.tex`.

---

## 5. Grafana — importação e configuração

**Export:** `docs/Dashboards/dashboard-grafana.json`

**Importar:**

1. http://localhost:3000 → **Dashboards → New → Import**
2. Carregar `docs/Dashboards/dashboard-grafana.json`
3. Datasource **Loki**: **Connections → Data sources → Add → Loki** → URL `http://loki:3100` (dentro do Compose)

**Producer e tasks** enviam logs para `http://localhost:3100/loki/api/v1/push` (host) ou `http://host.docker.internal:3100` (pods Flyte). Label: `application=tead`.

Queries úteis no **Explore**:

```logql
{application="tead", module="producer_network_events"} |= "event_sent"
{application="tead"} |= "table_insert" |= "phase=complete"
{application="tead", module="avaliar_streaming"} |= "Streaming QA: ALERTA"
```

Arrancar só observabilidade: `docker compose up -d loki grafana`

---

## 6. Superset — ligação e exportação

**URL:** http://localhost:8088 (`admin` / `admin`)

**Ligação Trino (dentro da rede Docker):**

| Campo | Valor |
|-------|-------|
| SQLAlchemy URI | `trino://flyte@trino:8080/iceberg` |
| (referência no repo) | `trino_uri_superset.txt` |

Registar datasets sobre `iceberg.gold.network_quality_daily`, `iceberg.gold.churn_risk_daily` e, se usar streaming, `iceberg.gold.network_events_hourly`.

**Exportar dashboard:** UI Superset → dashboard → **⋯ → Export** → guardar JSON em `docs/Dashboards/` (convenção do projeto).

**Build Superset:** `superset/Dockerfile` (driver Trino incluído); sobe com `docker compose up -d superset`.

---

## 7. Documentação e contratos (ficheiros no repo)

| Artefacto | Caminho |
|-----------|---------|
| Relatório LaTeX | `docs/Relatório/main.tex` |
| Limpeza Silver (apoio relatório) | `docs/Relatório/Ficheiro de limpeza da camada silver.md` |
| Construção Gold | `docs/Relatório/Ficheiro de construção da camada gold.md` |
| Data Products / Contratos v1 | `docs/Data_Products - Data_Contracts/v1/` |
| Data Products / Contratos v2 | `docs/Data_Products - Data_Contracts/v2/` |
| Governança | `docs/Data_Products - Data_Contracts/GOVERNANCA_DATA_PRODUCTS.md` |

---

## 8. Datasets

| Pasta | Conteúdo |
|-------|----------|
| `datasets/Datasets_Raw/` | CSV originais + `pipeline_raw_bronze.py`, `simular_tempestade.py` |
| `datasets/Datasets_Bronze_Leiria/` | Bronze Leiria (CSV) + `prepare_data.py` |
| `datasets/Datasets_Silver/` | Exemplos silver exportados (CSV) |

Bronze no MinIO para o batch: prefixos `warehouse/bronze/cdr_customers/`, `network_logs/`, `call_tests/`, `towers/`.

---

## 9. Scripts Python

| Script | Função |
|--------|--------|
| `python_scripts/producer_network_events.py` | Eventos sintéticos → tópico `network_events` |
| `python_scripts/requirements.txt` | Dependências (`kafka-python`, `python-logging-loki`, …) |

Opções do producer: `--no-nulls`, `--no-loki`, `--broker localhost:19092`.

---

## 10. SQL e catálogos Trino

| Ficheiro | Uso |
|----------|-----|
| `sql_scripts/setup_streaming_tables.sql` | Tabelas bronze/silver/gold streaming |
| `sql_scripts/migrate_streaming_dedup.sql` | Migração dedup / `ingest_batch_id` |
| `trino/etc/catalog/iceberg.properties` | Catálogo Iceberg |
| `trino/etc/catalog/kafka.properties` | Catálogo Kafka → Redpanda |
| `trino/etc/kafka/network_events.json` | Schema tópico `network_events` |

---

## Resolução de problemas

| Sintoma | Acção |
|---------|--------|
| `mc` termina logo | Normal; `docker compose logs mc` |
| Trino sem catálogos | `docker compose restart trino` após metastore |
| Flyte não alcança MinIO/Trino | `host.docker.internal`; ver secção 2 |
| Streaming bronze vazio | Producer antes de `jdpt_streaming_full_sync` |
| Falha Flyte opaca (`TypeError`) | Logs do pod; `pyflyte register` + repetir run |

```bash
docker compose logs -f trino
docker compose logs -f grafana
```

---

## Aviso de segurança

Credenciais por defeito (`minioadmin`, `admin/admin`) e portas em `localhost`. Não expor em redes públicas.
