# TEAD 2.0 — Local Data Processing Stack

A local data platform for development and experimentation, combining a **Docker Compose** stack (MinIO, Hive Metastore, Trino, MLflow) with a **Flyte Sandbox** container running independently as an external orchestration engine.

---

## Architecture Overview
```
┌─────────────────────────────────────────────────────┐
│               Docker Compose Stack                  │
│                                                     │
│  MinIO (S3) ──► Hive Metastore ──► Trino            │
│      │               │              ▲               │
│      └──► MLflow ◄───┘              │               │
│           (Postgres)         Redpanda (Kafka)       │
└─────────────────────────────────────────────────────┘
          ▲              ▲             ▲
          │  host ports  │             │
┌─────────────────────────────────────────────────────┐
│           Flyte Sandbox (external container)         │
│      (K3s inside Docker — built from flyte/)        │
└─────────────────────────────────────────────────────┘
```

**Services in Docker Compose:**

| Service | Description | Port(s) |
|---|---|---|
| `minio` | S3-compatible object storage | `9000` (API), `9001` (Console) |
| `mc` | One-shot bucket bootstrapper (exits on success) | — |
| `metastore-db` | MariaDB backend for Hive Metastore | internal |
| `hive-metastore` | Hive Metastore (Thrift) for Iceberg/Hive catalogs | `9083` |
| `trino` | Distributed query engine (Hive, Iceberg, **Kafka**) | `8080` |
| `redpanda` | Kafka-compatible bus para ingestão streaming | `19092` |
| `mlflow-db` | Postgres backend for MLflow | internal |
| `mlflow` | MLflow tracking server | `15000` |
| `loki` / `grafana` | Logs do pipeline Flyte | `3100` / `3000` |
| `superset` | BI / dashboards | `8088` |

**External container (Flyte):**

| Service | Description |
|---|---|
| Flyte Sandbox | Workflow orchestration (K3s inside Docker), built from `flyte/Dockerfile` |

---

## Repository Layout
```
.
├── docker-compose.yml
├── hive/
│   └── conf/                        # Hive Metastore configuration files
├── trino/
│   └── etc/
│       ├── config.properties        # Trino server config
│       ├── jvm.config
│       ├── node.properties
│       ├── log.properties
│       ├── catalog/
│       │   ├── hive.properties      # Hive catalog (backed by MinIO)
│       │   ├── iceberg.properties   # Iceberg catalog (backed by MinIO)
│       │   └── kafka.properties     # Kafka catalog (Redpanda)
│       └── kafka/                   # Schemas JSON dos tópicos Kafka
├── python_scripts/                  # Producer de eventos (streaming)
├── sql_scripts/                     # DDL streaming (bronze/silver/gold)
├── flyte-workflows/                 # @task/@workflow (raiz); helpers em workflow_functions/
├── docs/streaming.md                # Guia do ramo streaming
├── mlflow/
│   └── Dockerfile                   # Extends MLflow image with psycopg2
└── flyte/
    ├── Dockerfile                   # Patches Flyte sandbox entrypoint
    └── flyte-core-overrides.yaml    # Helm overrides for flyte-core
```

---

## Prerequisites

- **Docker Engine** (v20.10 or newer recommended)
- **Docker Compose v2** (`docker compose` — not the legacy `docker-compose`)
- **~8 GB RAM** available to Docker (Trino and Flyte are memory-intensive)
- **~10 GB free disk space** for images and volumes

---

## Quick Start

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd <repo-root>
```

### 2. Build and start the Docker Compose stack

The `mlflow` service requires a local build (adds `psycopg2` to the base MLflow image). Run:
```bash
docker compose up -d --build
```

This will:
- Pull all required images
- Build the `tead-mlflow:v2.12.1-pg` image locally
- Start all services in the background
- Run the `mc` bootstrapper to create the `warehouse` and `mlflow` buckets in MinIO (it will exit after completion — this is expected)

### 3. Verify the stack is healthy
```bash
docker compose ps
```

Check that all services (except `mc`, which exits normally) show status `running` or `Up`.

Verify bucket creation:
```bash
docker compose logs mc
```

You should see confirmation that the `warehouse` and `mlflow` buckets were created successfully.

---

## Service Endpoints

| Service | URL | Credentials |
|---|---|---|
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| MinIO S3 API | http://localhost:9000 | `minioadmin` / `minioadmin` |
| Trino UI | http://localhost:8080 | no auth |
| Hive Metastore (Thrift) | `thrift://localhost:9083` | — |
| MLflow | http://localhost:15000 | no auth |

---

## Validating the Stack

**MinIO:** Open http://localhost:9001 and confirm two buckets exist: `warehouse` and `mlflow`.

**Trino:** Verify the `hive` and `iceberg` catalogs are registered:
```bash
docker compose exec trino trino --execute "SHOW CATALOGS;"
```

Expected output (inclui catálogo **kafka** após adaptação streaming):
```
iceberg
hive
kafka
system
tpcds
tpch
```

## Streaming (Kafka / Redpanda)

O projeto inclui um ramo **streaming** (TEAD 2.0 v1.3) em paralelo com o lakehouse batch de telecom. Ver **[docs/streaming.md](docs/streaming.md)** para: subir Redpanda, criar tabelas, correr o producer e o workflow `jdpt_streaming_full_sync`.

**MLflow:** Open http://localhost:15000 — you should see the MLflow tracking UI with no experiments yet.

---

## Flyte Sandbox (External Container)

Flyte runs as a **separate container outside Docker Compose**. It contains its own internal Kubernetes (K3s), which is why it cannot simply join the Compose network.

For simplicity, we'll use a local Flyte cluster. To install a local Flyte cluster, follow the instructions at https://docs-legacy.flyte.org/en/v1.13.3/getting_started_with_workflow_development/running_a_workflow_locally.html#getting-started-running-workflow-local-cluster. You don't need to create a Flyte project yet, just install flytectl, start the demo, and wait for a few minuts to check Flyte is running (port 80030).

> **Note:** The sandbox startup takes several minutes as Helm installs Flyte components into K3s. Wait until you see log messages indicating the Flyte UI is ready before registering workflows.

### 3. Connecting Flyte Tasks to Compose Services

Flyte's internal K3s pods are isolated from the Docker Compose network. Since the Compose services publish ports on the host, Flyte tasks must reach them via the host machine.

Use `host.docker.internal` as the hostname from within Flyte task pods:

| Service | URL from Flyte tasks |
|---|---|
| MinIO S3 API | `http://host.docker.internal:9000` |
| MLflow | `http://host.docker.internal:15000` |
| Trino | `http://host.docker.internal:8080` |
| Hive Metastore | `thrift://host.docker.internal:9083` |

**MinIO credentials (same as Compose):**
- Access Key: `minioadmin`
- Secret Key: `minioadmin`

**MLflow artifact store:** configured to use MinIO. Set the following in your Flyte task environment:
```
MLFLOW_S3_ENDPOINT_URL=http://host.docker.internal:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
```

---

## Common Operations

**Tail logs for all services:**
```bash
docker compose logs -f
```

**Tail logs for a specific service:**
```bash
docker compose logs -f trino
docker compose logs -f hive-metastore
docker compose logs -f mlflow
```

**Rebuild only the MLflow image:**
```bash
docker compose build mlflow
docker compose up -d mlflow
```

**Stop the stack (preserves data volumes):**
```bash
docker compose down
```

**Full reset — stop and delete all data (MinIO objects, DBs, Trino data):**
```bash
docker compose down -v
```

---

## Troubleshooting

**`mc` container exits immediately**

This is expected. The `mc` service is a one-shot bootstrapper — it creates the MinIO buckets and exits with code 0. Check its logs to confirm success:
```bash
docker compose logs mc
```

**Trino starts but catalogs fail / no tables visible**

Trino starts before Hive Metastore is fully ready. Check Hive Metastore logs:
```bash
docker compose logs -f hive-metastore
```
Once it's healthy, restart Trino:
```bash
docker compose restart trino
```

**Flyte tasks cannot reach MinIO / MLflow / Trino**

This is a networking issue. Flyte tasks run inside K3s pods that are isolated from the Docker Compose network. Solution:
- macOS / Windows: use `host.docker.internal` — it works out of the box with Docker Desktop.
- Linux: ensure you started the Flyte sandbox with `--add-host host.docker.internal:host-gateway`, then use `host.docker.internal` as the hostname.

**Flyte sandbox fails to start / Helm timeout**

The patched image increases the Helm timeout to 30 minutes. If startup still fails, ensure Docker has enough RAM (8 GB minimum). On Docker Desktop, adjust this under **Settings → Resources**.

**`failed to read dockerfile: open .../Dockerfile: no such file or directory`**

Docker Compose expects `Dockerfile` (capital D). On case-sensitive filesystems (Linux), ensure the file is named `Dockerfile`, not `dockerfile`. Rename if needed:
```bash
mv mlflow/dockerfile mlflow/Dockerfile
mv flyte/dockerfile flyte/Dockerfile
```

---

## Security Notice

This stack uses hardcoded development credentials (`minioadmin/minioadmin`, `admin/admin`, etc.) and exposes all service ports on `localhost`. **Do not deploy this configuration to shared or public-facing environments.**

---

## Service Dependency Graph
```
minio
 └── mc (bucket bootstrap, then exits)
 └── hive-metastore
      └── metastore-db (MariaDB)
 redpanda
      └── trino (hive + kafka catalogs)
 └── mlflow
      └── mlflow-db (Postgres)

[external]
flyte-sandbox → connects to Compose services via host ports
```