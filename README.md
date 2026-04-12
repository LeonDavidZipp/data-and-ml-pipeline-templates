# datascience-as-a-service

> **Note:** This project is not yet tested. Tests will be added incrementally.

An end-to-end data science platform built on **Dagster**, following a **medallion architecture** (bronze → silver → gold) with Delta Lake storage on S3/MinIO, ML training via any ML framework + Optuna, and experiment tracking with MLflow.

---

## Architecture

```mermaid
graph LR
    Bronze["Bronze"] --> Silver["Silver"]
    Silver --> Gold["Gold"]
    Gold --> MLflow["MLflow"]
```

Each layer reads from and writes to **Delta Lake tables** on S3 (MinIO) using UPSERT merge semantics for idempotent writes. Downstream layers use Dagster's `AutomationCondition.eager()` to auto-materialise when upstream assets complete.

---

## Infrastructure (Docker Compose)

| Service               | Port        | Description                                                          |
| --------------------- | ----------- | -------------------------------------------------------------------- |
| **MinIO**             | 9000 / 8900 | S3-compatible object storage                                         |
| **PostgreSQL**        | 5432        | Dagster run/event storage &amp; MLflow backend                       |
| **MLflow**            | 5000        | Experiment tracking server                                           |
| **Dagster webserver** | 3000        | Dagster UI &amp; scheduler                                           |
| **Dagster daemon**    | —           | Runs schedules, sensors, and the run queue                           |
| **Dagster user code** | gRPC        | User code execution server                                           |
| **createbuckets**     | —           | Init container: creates `mlflow`, `bronze`, `silver`, `gold` buckets |

Start everything:

```bash
docker compose up
```

---

## Assets

### Bronze (ingestion)

| Asset             | Source          | Method                      |
| ----------------- | --------------- | --------------------------- |
| `ingest_postgres` | PostgreSQL      | connectorx → Delta (UPSERT) |
| `ingest_duckdb`   | DuckDB          | duckdb → Delta              |
| `ingest_http`     | REST API        | httpx → Delta               |
| `ingest_csv`      | S3 CSV file     | s3fs → Polars               |
| `ingest_parquet`  | S3 Parquet file | s3fs → Polars               |
| `ingest_xlsx`     | S3 Excel file   | s3fs → Polars               |
| `ingest_json`     | S3 JSON file    | s3fs → Polars               |
| `ingest_ndjson`   | S3 NDJSON file  | s3fs → Polars               |

### Silver / Gold

- **Silver** — Deduplication, null handling, type casting, joins, business logic
- **Gold** — Final aggregations and feature sets for ML

### ML (MLflow)

- **`assets_mlflow`** — XGBoost regressor with Optuna hyperparameter tuning (50 trials). Logs params, metrics (`train_rmse`, `test_rmse`, `test_mae`, `test_r2`), and the model to MLflow.

---

## Resources

| Key             | Class              | Description                                                 |
| --------------- | ------------------ | ----------------------------------------------------------- |
| `postgres`      | `PostgresResource` | PostgreSQL connection                                       |
| `duckdb_source` | `DuckDBResource`   | DuckDB file connector                                       |
| `http`          | `HttpResource`     | HTTP API client with token auth                             |
| `s3`            | `S3Resource`       | S3/MinIO access (Delta storage options + `s3fs` filesystem) |
| `mlflow`        | `mlflow_tracking`  | MLflow tracking (dagster-mlflow)                            |

All secrets are injected via `dg.EnvVar` — no plaintext credentials.

---

## Checks

- **Row-count checks** for every bronze ingestor (`check_ingest_*_not_empty`)
- **`check_model_improvement`** — compares `test_rmse` across the last two materialisations

## Schedules &amp; Sensors

- **`daily_ingest_schedule`** — cron `0 0 * * *`, targets all assets
- **`raw_sensor`** — sensor-based trigger, targets all assets

---

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) &amp; Docker Compose
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### Install

```bash
uv sync
source .venv/bin/activate
```

Or with pip:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Run

```bash
# start infrastructure
docker compose up -d

# start Dagster dev server
dg dev
```

Open http://localhost:3000 in your browser.

---

## Tech Stack

| Category            | Tools                         |
| ------------------- | ----------------------------- |
| Orchestration       | Dagster 1.12                  |
| DataFrames          | Polars                        |
| Storage             | Delta Lake on S3 (MinIO)      |
| ML                  | XGBoost, Optuna, scikit-learn |
| Experiment tracking | MLflow                        |
| API client          | httpx                         |
| Linting             | Ruff                          |
