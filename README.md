# E-Commerce Data Engineering Platform

An end-to-end Data Engineering portfolio project that implements a structured data platform for an e-commerce scenario.

The project is rebuilt and extended from an academic Data Engineering capstone with an emphasis on understanding, implementing, validating, and documenting each component using professional engineering practices.

## Project Goal

Design and implement a data platform that separates transactional workloads from analytical processing, automates data movement, preserves historical dimension changes, and prepares analytical data for business intelligence reporting.

## Architecture

The core data flow is:

`MySQL OLTP → Python ETL → PostgreSQL Data Warehouse → Power BI`

Apache Airflow orchestrates the ETL workflows.

```text
MySQL OLTP
    │
    ▼
Python ETL
    │
    ├── Full load
    ├── Incremental load
    ├── Composite watermark
    └── Slowly Changing Dimensions
    │
    ▼
PostgreSQL Data Warehouse
    │
    ├── Dimensional model
    ├── Historical dimensions
    ├── Temporal fact resolution
    └── ETL audit
    │
    ▼
Power BI

Apache Airflow
    │
    └── ETL orchestration and monitoring

```

## Project Modules

- `01-oltp-database` — Operational sales database implemented with MySQL.
- `02-etl-pipelines` — Python ETL pipelines for full and incremental data processing.
- `03-data-warehouse` — Dimensional Data Warehouse implemented with PostgreSQL.
- `04-business-intelligence` — Analytical modeling and reporting with Power BI.
- `airflow` — Workflow orchestration and monitoring with Apache Airflow.

The `05-big-data` directory is retained in the repository structure, but Spark/PySpark processing is intentionally outside the current project scope.

## Implemented Features

- Full ETL load from MySQL to PostgreSQL.
- Incremental loading with a composite `(updated_at, order_id)` watermark.
- Idempotent fact loading with PostgreSQL upserts.
- Slowly Changing Dimensions using Type 0, Type 1, and Type 2 strategies.
- Historical dimension versioning with temporal validity intervals.
- Temporal surrogate-key resolution for fact records.
- SHA-256 change detection for selected Type 2 attributes.
- ETL audit and reconciliation controls.
- Apache Airflow orchestration with explicit task dependencies.
- Controlled failure handling and retry-safe audit behavior.
- Automated ETL and SCD tests.
- Fresh-database reproducibility validation.

## Running the Project

### Prerequisites

The project requires:

- Docker and Docker Compose
- Python 3.10
- Git

### Environment Setup

Clone the repository and enter the project directory:

```bash
git clone https://github.com/DiegoSuarz/ecommerce-data-engineering-platform.git
cd ecommerce-data-engineering-platform
```

Create the local environment configuration:

```bash
cp .env.example .env
```

Set the required passwords and secrets in `.env` before starting the services.

Create and activate a Python virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install the Python dependencies:

```bash
pip install -r requirements.txt
```

### Infrastructure Setup

Start the Docker services:

```bash
docker compose up -d
```

The Docker Compose environment starts:

- MySQL for the OLTP source database.
- PostgreSQL for the analytical Data Warehouse.
- A separate PostgreSQL database for Airflow metadata.
- Apache Airflow API server, scheduler, and DAG processor.

On a fresh database initialization, the SQL scripts under `01-oltp-database/sql` and `03-data-warehouse/sql` are executed automatically in migration order.

Airflow database migrations are executed by the `airflow-init` service before the Airflow runtime services start.

Check the service status:

```bash
docker compose ps
```

### Manual ETL Execution

The ETL pipelines can be executed directly from the local Python environment without Airflow.

Run the initial full load:

```bash
python 02-etl-pipelines/src/full_load.py
```

The full load extracts the complete source dataset, loads the staging layer, performs data quality checks, builds the dimensions, resolves historical dimension relationships, loads the fact table, and records the execution in the ETL audit tables.

After the initial load, run incremental processing with:

```bash
python 02-etl-pipelines/src/incremental_load.py
```

The incremental pipeline uses a composite `(updated_at, order_id)` watermark to extract new or changed orders. Dimension processing is executed independently from the presence of new order records so that Slowly Changing Dimension changes can still be detected.

The watermark is advanced only after the incremental batch passes the required loading, quality, and reconciliation steps.

### Airflow Orchestration

Apache Airflow orchestrates the incremental ETL workflow through the `ecommerce_incremental_load` DAG.

The DAG is scheduled daily at `02:00` using the cron expression:

```text
0 2 * * *
```

It uses explicit task dependencies, ETL audit tracking, data quality checks, reconciliation, and controlled watermark advancement.

The DAG also sets:

- `catchup=False`
- `max_active_runs=1`

These settings avoid automatic historical backfills and prevent overlapping incremental pipeline executions.

Available DAGs include:

- `ecommerce_incremental_load` — Main incremental ETL orchestration.
- `ecommerce_connectivity_check` — Validates database connectivity.
- `ecommerce_smoke_test` — Basic Airflow environment validation.
- `ecommerce_audit_reconciliation` — Reconciles stale or incomplete ETL audit executions.

After starting the Docker services, list the available DAGs with:

```bash
docker compose exec airflow-scheduler airflow dags list
```

Trigger the incremental pipeline manually with:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger ecommerce_incremental_load
```

Check recent DAG runs with:

```bash
docker compose exec airflow-scheduler \
  airflow dags list-runs ecommerce_incremental_load
```

The Airflow web interface is available through the port configured by `AIRFLOW_PORT` in `.env`.

## Documentation

Additional technical documentation is available in:

- [`01-oltp-database/README.md`](01-oltp-database/README.md) — OLTP database structure and source data setup.
- [`02-etl-pipelines/README.md`](02-etl-pipelines/README.md) — ETL pipeline implementation and execution details.
- [`airflow/README.md`](airflow/README.md) — Airflow orchestration setup and DAG usage.
- [`docs/scd-design.md`](docs/scd-design.md) — Slowly Changing Dimensions design, temporal validity, hashing strategy, migration behavior, and validation.

## Status

🚧 **Active development**

The core data platform is operational and currently includes:

- MySQL OLTP source database.
- PostgreSQL dimensional Data Warehouse.
- Full and incremental ETL pipelines.
- Composite watermark-based incremental processing.
- ETL audit, quality checks, and reconciliation.
- Apache Airflow orchestration.
- Slowly Changing Dimensions with Type 0, Type 1, and Type 2 behavior.
- Historical and temporal dimension resolution.

The next development stages will extend the platform with Change Data Capture (CDC) and business intelligence reporting.
