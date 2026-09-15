# E-Commerce Data Engineering Platform

An end-to-end Data Engineering portfolio project that implements a structured data platform for an e-commerce scenario.

The project is rebuilt and extended from an academic Data Engineering capstone with an emphasis on understanding, implementing, validating, and documenting each component using professional engineering practices.

## Project Goal

Design and implement a data platform that separates transactional workloads from analytical processing, automates data movement, preserves historical dimension changes, and prepares analytical data for business intelligence reporting.

## Architecture

The final platform uses two complementary processing responsibilities:

* **Full Load** for initial warehouse bootstrap and complete reconstruction.
* **Change Data Capture (CDC)** for continuous synchronization after bootstrap.

```text
MySQL OLTP
    │
    ├──────────────────── Full Load ────────────────────┐
    │                                                  │
    │                                                  ▼
    │                                      PostgreSQL Data Warehouse
    │
    ▼
MySQL Binary Log
    │
    ▼
CDC EXTRACT
    │
    ├── durable RAW events
    └── READ checkpoint
    │
    ▼
cdc.raw_change_event
    │
    ▼
CDC TRANSFORM
    │
    ▼
cdc.transformed_event
    │
    ▼
CDC LOAD / APPLY
    │
    ├── cdc.change_event
    ├── Data Warehouse mutation
    └── APPLY checkpoint
```

The LOAD/APPLY stage persists final CDC events, applies their analytical
effects to the dimensional warehouse, and advances the APPLY checkpoint
within the same PostgreSQL transaction.

Apache Airflow orchestrates CDC as a multi-stage pipeline with
`max_active_runs=1`, preserving event ordering and preventing overlapping
consumers.

## Project Modules

- `01-oltp-database` — Operational sales database implemented with MySQL.
- `02-etl-pipelines` — Python Full Load and CDC pipelines for warehouse bootstrap and continuous synchronization.
- `03-data-warehouse` — Dimensional Data Warehouse implemented with PostgreSQL.
- `04-business-intelligence` — Analytical modeling and reporting with Power BI.
- `airflow` — Workflow orchestration and monitoring with Apache Airflow.

The `05-big-data` directory is retained in the repository structure, but Spark/PySpark processing is intentionally outside the current project scope.

## Implemented Features

- Full ETL load from MySQL to PostgreSQL.
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
- Log-based MySQL Change Data Capture for INSERT, UPDATE, and DELETE operations.
- Transaction-aware CDC processing based on committed MySQL binlog transactions.
- Durable RAW, TRANSFORMED, and FINAL CDC event layers in PostgreSQL.
- Dual READ/APPLY CDC checkpoints with atomic state progression.
- Safe checkpoint advancement across empty batches and binlog rotation.
- Idempotent CDC persistence using deterministic event keys.
- Multi-stage Airflow CDC orchestration with explicit EXTRACT, TRANSFORM, and LOAD tasks.

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

The Full Load can be executed directly from the local Python environment.

Run the warehouse bootstrap or reconstruction with:

```bash
python 02-etl-pipelines/src/full_load.py
```

The Full Load extracts the complete source dataset, loads staging,
performs data quality checks, builds dimensions, resolves temporal
relationships, loads the fact table, and records the execution in the
ETL audit tables.

After bootstrap, ongoing source synchronization is owned by the CDC
pipeline rather than by periodic source-table polling.

### Airflow Orchestration

Apache Airflow orchestrates the continuous CDC workflow through the
`ecommerce_change_data_capture` DAG.

The CDC DAG runs every five minutes and uses:

* explicit EXTRACT, TRANSFORM, LOAD, COMPLETE, and failure-finalization stages;
* durable PostgreSQL RAW, TRANSFORMED, and FINAL event layers;
* READ and APPLY binlog checkpoints;
* transactionally coupled Data Warehouse mutation and APPLY advancement;
* `max_active_runs=1` to preserve ordered single-consumer processing.

The repository also contains:

* `ecommerce_audit_reconciliation` — reconciles stale CDC audit executions;
* `ecommerce_connectivity_check` — validates database connectivity;
* `ecommerce_smoke_test` — validates the Airflow runtime environment.

List the active DAG definitions directly from the current bundle with:

```bash
docker compose exec airflow-scheduler \
  airflow dags list -l -B dags-folder
```

The Airflow web interface is exposed through the port configured by
`AIRFLOW_PORT` in `.env`.

## Documentation

Additional technical documentation is available in:

- [`01-oltp-database/README.md`](01-oltp-database/README.md) — OLTP database structure and source data setup.
- [`02-etl-pipelines/README.md`](02-etl-pipelines/README.md) — ETL and CDC pipeline implementation and execution details.
- [`airflow/README.md`](airflow/README.md) — Airflow orchestration setup and DAG usage.
- [`docs/scd-design.md`](docs/scd-design.md) — Slowly Changing Dimensions design, temporal validity, hashing strategy, migration behavior, and validation.
- [`docs/cdc-design.md`](docs/cdc-design.md) — Log-based CDC architecture, transaction handling, durable staging, checkpoint model, failure semantics, and end-to-end validation.

## Status

🚧 **Active development**

The core data platform is operational and currently includes:

- MySQL OLTP source database.
- PostgreSQL dimensional Data Warehouse.
- Full Load bootstrap and CDC-based continuous synchronization.
- ETL audit, quality checks, and reconciliation.
- Apache Airflow orchestration.
- Slowly Changing Dimensions with Type 0, Type 1, and Type 2 behavior.
- Historical and temporal dimension resolution.
- Log-based Change Data Capture from the MySQL binary log.
- INSERT, UPDATE, and DELETE event capture.
- Durable multi-stage CDC processing through RAW, TRANSFORMED, and FINAL layers.
- READ/APPLY checkpoint management with retry-safe and idempotent persistence.

The next development stages will focus on pipeline hardening, end-to-end operational validation, and final portfolio documentation.
