# E-Commerce Data Engineering Platform

An end-to-end Data Engineering portfolio project that implements a reproducible data platform for an e-commerce scenario using **MySQL, PostgreSQL, Python, Apache Airflow, Docker, Slowly Changing Dimensions, and log-based Change Data Capture**.

The platform separates transactional and analytical workloads, bootstraps a dimensional Data Warehouse through a Full Load pipeline, and continuously synchronizes source changes through a transaction-aware CDC pipeline orchestrated by Airflow.

**Latest stable release:** `v0.7.0`

---

## Project Goal

The project demonstrates how to design and operate a small production-oriented data platform that:

* separates OLTP workloads from analytical processing;
* performs deterministic warehouse bootstrap and reconstruction;
* preserves historical dimension changes;
* resolves temporal surrogate keys for facts;
* captures committed changes from the MySQL binary log;
* propagates INSERT, UPDATE, and DELETE operations into the analytical warehouse;
* tracks durable processing progress explicitly;
* provides audit, reconciliation, retry, and failure-handling controls;
* can be rebuilt from a clean local environment using Docker.

---

## Architecture

The final architecture has two complementary processing responsibilities:

* **Full Load** — initial Data Warehouse bootstrap or complete reconstruction.
* **Change Data Capture (CDC)** — continuous synchronization after bootstrap.

```text
                         ┌──────────────────────┐
                         │      MySQL OLTP      │
                         └──────────┬───────────┘
                                    │
                    ┌───────────────┴────────────────┐
                    │                                │
                    │ Full Load                      │ MySQL ROW Binlog
                    │                                │
                    ▼                                ▼
           ┌─────────────────┐              ┌─────────────────┐
           │     Staging     │              │   CDC EXTRACT   │
           └────────┬────────┘              └────────┬────────┘
                    │                                │
                    ▼                                ├── RAW events
           ┌─────────────────┐                       └── READ checkpoint
           │ Dimensional DW  │                                │
           │                 │                                ▼
           │ dim_date        │                       CDC TRANSFORM
           │ dim_category    │                                │
           │ dim_country     │                                ▼
           │ fact_sales      │                         TRANSFORMED
           └─────────────────┘                                │
                                                             ▼
                                                    CDC LOAD / APPLY
                                                       │    │    │
                                                       │    │    └── APPLY checkpoint
                                                       │    └─────── DW mutation
                                                       └──────────── FINAL events
```

Apache Airflow orchestrates the continuous pipeline as:

```text
START
  ↓
EXTRACT
  ↓
TRANSFORM
  ↓
LOAD / APPLY
  ↓
COMPLETE
```

A failure-finalization path records unsuccessful executions.

The CDC DAG uses:

```text
max_active_runs = 1
```

to preserve ordered single-consumer processing.

---

## Engineering Highlights

### Log-Based CDC

The platform consumes committed MySQL ROW-binlog transactions and supports:

* INSERT;
* UPDATE;
* DELETE;
* transaction boundaries;
* binlog rotation;
* empty batches;
* deterministic event identity;
* retry-safe persistence.

CDC events pass through three durable PostgreSQL layers:

```text
RAW
  ↓
TRANSFORMED
  ↓
FINAL
```

Event payloads remain in PostgreSQL while Airflow exchanges only small execution metadata between tasks.

### Dual Checkpoint Model

CDC separates source consumption from final application through two checkpoints:

```text
READ
    mysql_sales_binlog_read
    → source events safely persisted in RAW

APPLY
    mysql_sales_binlog
    → events persisted in FINAL and reflected in the Data Warehouse
```

A new batch normally begins only when:

```text
READ == APPLY
```

If durable pending work or inconsistent audit state exists, the pipeline fails closed rather than advancing uncertain state.

### Transactional Progression

CDC protects processing state with two transactional boundaries:

```text
EXTRACT
RAW persistence + READ checkpoint
```

and:

```text
LOAD / APPLY
FINAL persistence + DW mutation + APPLY checkpoint
```

This prevents processing checkpoints from advancing independently of the data they represent.

### Slowly Changing Dimensions

The dimensional model implements:

```text
SCD0 → immutable business attributes
SCD1 → corrections propagated across history
SCD2 → historical version creation
```

Category and country dimensions maintain temporal validity intervals, current-row indicators, and deterministic Type 2 change detection.

Fact records resolve dimension surrogate keys according to the dimension version valid on the order business date rather than the ETL execution date.

### Reliability Controls

The platform includes:

* committed-transaction CDC processing;
* durable RAW / TRANSFORMED / FINAL stages;
* deterministic CDC event keys;
* idempotent warehouse writes;
* READ/APPLY divergence detection;
* latest CDC batch readiness validation;
* retry-safe committed LOAD replay;
* ETL audit state transitions;
* stale-run reconciliation;
* stage-specific Airflow retries;
* fail-closed handling for inconsistent durable state.

---

## Technology Stack

| Layer            | Technology                     |
| ---------------- | ------------------------------ |
| OLTP source      | MySQL 8                        |
| Data Warehouse   | PostgreSQL 16                  |
| ETL / CDC        | Python 3.10                    |
| Orchestration    | Apache Airflow 3               |
| CDC source       | MySQL binary log               |
| Containerization | Docker / Docker Compose        |
| Testing          | pytest                         |
| Configuration    | Environment variables / dotenv |
| Version control  | Git / GitHub                   |

---

## Repository Structure

```text
.
├── 01-oltp-database/
│   ├── data/
│   ├── sql/
│   └── README.md
│
├── 02-etl-pipelines/
│   ├── src/
│   │   ├── cdc/
│   │   ├── full_load.py
│   │   ├── audit.py
│   │   ├── extract.py
│   │   ├── transform.py
│   │   ├── load.py
│   │   └── quality.py
│   ├── tests/
│   └── README.md
│
├── 03-data-warehouse/
│   └── sql/
│
├── airflow/
│   ├── dags/
│   ├── scripts/
│   ├── Dockerfile
│   └── README.md
│
├── docs/
│   ├── cdc-design.md
│   └── scd-design.md
│
├── compose.yaml
├── requirements.txt
└── README.md
```

Main responsibilities:

| Module              | Responsibility                                                           |
| ------------------- | ------------------------------------------------------------------------ |
| `01-oltp-database`  | MySQL source schema, seed data, and CDC user setup                       |
| `02-etl-pipelines`  | Full Load, CDC, SCD, audit, reconciliation, and quality logic            |
| `03-data-warehouse` | PostgreSQL staging, dimensional, audit, and CDC schemas                  |
| `airflow`           | orchestration, scheduling, connectivity checks, and connection bootstrap |
| `docs`              | detailed architecture and engineering documentation                      |

---

## Validation Evidence

The complete platform was rebuilt from empty Docker volumes to validate fresh-install reproducibility.

### Full Load

Fresh source:

```text
categories = 5
countries  = 56
orders     = 300000
```

Resulting Data Warehouse:

```text
dim_category = 5
dim_country  = 56
dim_date     = 1096
fact_sales   = 300000
```

### CDC Bootstrap

A fresh CDC installation safely initialized READ and APPLY from the current MySQL binary-log head.

The first zero-event execution completed with:

```text
transactions = 0
events       = 0

READ == APPLY
```

### CDC Mutation Probe

A real mutation probe processed:

```text
transactions = 6
events       = 18

INSERT = 6
UPDATE = 6
DELETE = 6
```

All durable layers converged:

```text
RAW         = 18
TRANSFORMED = 18
FINAL       = 18
```

The final state satisfied:

```text
READ == APPLY
```

SCD history was preserved, the final DELETE removed the probe fact, and a subsequent scheduled CDC execution completed successfully as a zero-event no-op.

### Regression Suite

The `v0.7.0` baseline passes:

```text
122 passed
```

---

## Quick Start

### Prerequisites

* Docker
* Docker Compose
* Python 3.10
* Git

Clone the repository:

```bash
git clone https://github.com/DiegoSuarz/ecommerce-data-engineering-platform.git
cd ecommerce-data-engineering-platform
```

Create the local configuration:

```bash
cp .env.example .env
```

Set the required database passwords and Airflow secrets in `.env`.

Create the Python environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Start the infrastructure:

```bash
docker compose up -d
```

Verify the services:

```bash
docker compose ps
```

Run the initial warehouse bootstrap:

```bash
python 02-etl-pipelines/src/full_load.py
```

List the Airflow DAGs:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags list \
  -l \
  -B dags-folder
```

After the Full Load and connectivity checks succeed, the CDC DAG can be enabled for continuous synchronization.

---

## Airflow Workflows

| DAG                              | Purpose                                                  |
| -------------------------------- | -------------------------------------------------------- |
| `ecommerce_change_data_capture`  | Multi-stage CDC synchronization from MySQL to PostgreSQL |
| `ecommerce_audit_reconciliation` | Reconciliation of stale ETL audit executions             |
| `ecommerce_connectivity_check`   | Manual validation of database connectivity               |

The CDC workflow runs every five minutes by default and uses `max_active_runs=1` to prevent overlapping binlog consumers.

---

## Documentation

Detailed technical documentation is available in:

* [`01-oltp-database/README.md`](01-oltp-database/README.md) — source database structure and initialization.
* [`02-etl-pipelines/README.md`](02-etl-pipelines/README.md) — Full Load, SCD, CDC, audit, quality, and implementation details.
* [`airflow/README.md`](airflow/README.md) — Airflow architecture, connections, DAGs, retries, and scheduling.
* [`docs/scd-design.md`](docs/scd-design.md) — SCD strategy, temporal validity, hashing, and validation.
* [`docs/cdc-design.md`](docs/cdc-design.md) — CDC architecture, durable stages, checkpoints, failure semantics, and end-to-end validation.

---

## Release Evolution

| Release  | Milestone                                           |
| -------- | --------------------------------------------------- |
| `v0.1.0` | Full Load pipeline                                  |
| `v0.2.0` | Simple incremental watermark                        |
| `v0.3.0` | Composite watermark                                 |
| `v0.4.0` | Apache Airflow orchestration                        |
| `v0.5.0` | Slowly Changing Dimensions                          |
| `v0.6.0` | Multi-stage Change Data Capture                     |
| `v0.7.0` | CDC-first consolidation and fresh-install hardening |

Earlier implementations remain available through Git history and release tags, while `main` represents the current architecture.

---

## Project Status

**Stable portfolio baseline — `v0.7.0`**

The implemented platform includes:

```text
MySQL OLTP                    ✅
Full Load bootstrap           ✅
PostgreSQL dimensional DW     ✅
SCD0 / SCD1 / SCD2            ✅
Log-based CDC                 ✅
INSERT / UPDATE / DELETE      ✅
READ / APPLY checkpoints      ✅
Airflow orchestration         ✅
Audit and reconciliation      ✅
Fresh-install reproducibility ✅
122-test regression baseline  ✅
```

The final processing model is:

```text
Full Load
    → bootstrap / reconstruction

CDC + Airflow
    → continuous synchronization
```

The repository is maintained as a Data Engineering portfolio project focused on architecture, data integrity, reproducibility, and technical documentation.
