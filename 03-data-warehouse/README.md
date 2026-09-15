# Data Warehouse

This module defines the PostgreSQL analytical storage layer for the E-Commerce Data Engineering Platform.

The warehouse is initialized through ordered SQL scripts in [`sql/`](sql/) and supports the Full Load bootstrap, Slowly Changing Dimensions, audit metadata, and the multi-stage CDC pipeline.

## Role in the Platform

PostgreSQL is the durable analytical target of the platform.

The current architecture uses:

* **Full Load** for initial warehouse bootstrap and reconstruction.
* **CDC** for continuous synchronization after bootstrap.
* **Audit metadata** for pipeline execution history and operational reconciliation.
* **CDC metadata and staging layers** for durable event processing and checkpoint progression.

## Schemas

The warehouse uses four logical schemas:

| Schema    | Purpose                                                 |
| --------- | ------------------------------------------------------- |
| `staging` | Intermediate Full Load and transformation structures    |
| `dw`      | Dimensional analytical model                            |
| `audit`   | ETL and CDC execution metadata                          |
| `cdc`     | Durable CDC events, transformed events, and checkpoints |

## Dimensional Model

The analytical model is centered on:

* `dw.dim_date`
* `dw.dim_category`
* `dw.dim_country`
* `dw.fact_sales`

`fact_sales` references the date, country, and category dimensions through warehouse surrogate keys.

Category and country dimensions support the project's SCD strategy, including temporal validity for historical dimension resolution.

## CDC Storage

The CDC subsystem persists processing state in PostgreSQL rather than relying on Airflow task state alone.

Its durable layers support:

```text
MySQL ROW binlog
    → RAW
    → TRANSFORMED
    → FINAL
    → Data Warehouse mutation
```

CDC progress is tracked through separate READ and APPLY checkpoints.

After a successful synchronized batch, the core health invariant is:

```text
READ == APPLY
```

## SQL Initialization

The SQL files under [`sql/`](sql/) are applied in filename order when a clean PostgreSQL data volume is initialized.

The sequence evolved with the project, so numbering gaps are intentionally preserved as part of the repository history rather than renumbered.

The scripts cover:

* schema creation;
* staging tables;
* dimensional tables;
* audit metadata;
* SCD attributes and constraints;
* orchestration hardening;
* CDC metadata;
* durable CDC staging tables.

## Docker Initialization

The PostgreSQL service mounts:

```text
./03-data-warehouse/sql
    → /docker-entrypoint-initdb.d
```

Therefore, on a fresh PostgreSQL volume, the database image automatically initializes the warehouse from these SQL files.

For a complete clean-environment procedure, see:

* [`../docs/local-reproduction.md`](../docs/local-reproduction.md)
* [`../docs/airflow-operations.md`](../docs/airflow-operations.md)

For detailed dimensional and CDC semantics, see:

* [`../docs/scd-design.md`](../docs/scd-design.md)
* [`../docs/cdc-design.md`](../docs/cdc-design.md)

## Current Baseline

The current stable architecture treats the Data Warehouse as:

```text
Full Load
    → bootstrap / reconstruction

CDC
    → continuous analytical synchronization
```

Historical watermark-based incremental implementations remain available through Git history and earlier release tags, but they are not part of the current runtime architecture.
