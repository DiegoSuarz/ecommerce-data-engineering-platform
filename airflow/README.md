# Airflow Orchestration

## Overview

Apache Airflow orchestrates the operational workflows of the
E-Commerce Data Engineering Platform.

Its primary production responsibility is the multi-stage Change Data
Capture pipeline that continuously synchronizes MySQL source changes
into PostgreSQL CDC storage and the analytical Data Warehouse.

Airflow also provides stale-run reconciliation, connectivity checks,
smoke validation, retry policies, and orchestration metadata.

## Architecture

```text
MySQL Binary Log
        │
        ▼
ecommerce_change_data_capture
        │
        ├── start_cdc
        ├── extract_cdc
        ├── transform_cdc
        ├── load_cdc
        ├── complete_cdc
        └── fail_cdc
                │
                ▼
           PostgreSQL
                │
                ├── CDC durable layers
                ├── Data Warehouse
                └── audit metadata

ecommerce_audit_reconciliation
        │
        └── marks stale CDC audit runs as failed

ecommerce_connectivity_check
        │
        └── operational validation
```

## Docker Services

The Airflow environment is defined in the root `compose.yaml` file.

The main services are:

| Service | Purpose |
|---|---|
| `airflow-api-server` | Provides the Airflow API and web interface |
| `airflow-scheduler` | Schedules DAG runs and tasks |
| `airflow-dag-processor` | Parses and processes DAG definitions |
| `airflow-postgres` | Stores Airflow internal metadata |
| `ecommerce-mysql` | MySQL OLTP source database |
| `ecommerce-postgres` | PostgreSQL data warehouse |

Airflow uses the `LocalExecutor`.

The Airflow runtime uses a custom image derived from the configured Apache Airflow base image.

The custom image is defined in:

```text
airflow/Dockerfile
```

Additional Python packages required by CDC are defined in:

```text
airflow/requirements.txt
```

This includes the MySQL binary-log replication client used by the CDC reader.

The ETL source code is mounted inside the Airflow containers as:

```text
./02-etl-pipelines/src -> /opt/airflow/etl
```

The DAG directory is mounted as:

```text
./airflow/dags -> /opt/airflow/dags
```

---

## Environment Variables

Airflow configuration values are defined through environment variables.

The `.env.example` file contains the required variable names:

```text
AIRFLOW_BASE_IMAGE
AIRFLOW_IMAGE_NAME
AIRFLOW_UID

AIRFLOW_POSTGRES_DATABASE
AIRFLOW_POSTGRES_USER
AIRFLOW_POSTGRES_PASSWORD

AIRFLOW_PORT
AIRFLOW_JWT_SECRET
AIRFLOW_FERNET_KEY

MYSQL_CDC_SERVER_ID
```

Real credentials and secrets must be stored in the local `.env` file.

The `.env` file must never be committed to Git.

---

## Airflow Connections

The DAGs use Airflow Connections instead of hardcoded database credentials.

Three database connections are used.

### MySQL source

```text
Connection ID: mysql_source
```

This connection points to the MySQL OLTP database and is used by the standard ETL pipelines.

### MySQL CDC source

```text
Connection ID: mysql_cdc_source
```

This connection is used exclusively by the CDC pipeline.

It points to the same MySQL source database but uses the dedicated CDC account configured with the minimum privileges required to:

* read the monitored `sales` tables;
* consume the MySQL replication stream.

The CDC account is separate from the standard ETL source connection and does not have privileges to modify source data.

### PostgreSQL data warehouse

```text
Connection ID: postgres_dw
```

This connection points to the PostgreSQL data warehouse.

The DAG retrieves the connection information through Airflow hooks and makes
the credentials available to the existing ETL modules through environment
variables.

This keeps database credentials outside the Python source code.

---

### Creating the connections

Load the local environment variables:

```bash
set -a
source .env
set +a
```

Create the MySQL source connection:

```bash
docker compose exec airflow-scheduler \
  airflow connections add mysql_source \
  --conn-type mysql \
  --conn-host mysql \
  --conn-port 3306 \
  --conn-schema "$MYSQL_DATABASE" \
  --conn-login "$MYSQL_USER" \
  --conn-password "$MYSQL_PASSWORD"
```

Create the dedicated MySQL CDC source connection:

    docker compose exec airflow-scheduler \
      airflow connections add mysql_cdc_source \
      --conn-type mysql \
      --conn-host mysql \
      --conn-port 3306 \
      --conn-schema "$MYSQL_DATABASE" \
      --conn-login "$MYSQL_CDC_USER" \
      --conn-password "$MYSQL_CDC_PASSWORD"

Create the PostgreSQL data warehouse connection:

```bash
docker compose exec airflow-scheduler \
  airflow connections add postgres_dw \
  --conn-type postgres \
  --conn-host postgres \
  --conn-port 5432 \
  --conn-schema "$POSTGRES_DATABASE" \
  --conn-login "$POSTGRES_USER" \
  --conn-password "$POSTGRES_PASSWORD"
```

Verify them:

```bash
docker compose exec airflow-scheduler \
  airflow connections list
```
> The current authentication setup is intended for local development only.
> Use a production-grade Airflow authentication manager for production deployments.


## DAGs

The project currently contains three Airflow DAGs.



### ecommerce_change_data_capture

Multi-stage log-based CDC orchestration DAG.

```text
Schedule: every 5 minutes
Max active runs: 1
```

The DAG coordinates the CDC pipeline through six Airflow tasks:

```text
start_cdc
    |
    v
extract_cdc
    |
    v
transform_cdc
    |
    v
load_cdc
    |
    v
complete_cdc
```

A failure-finalization task is also connected to the processing stages:

```text
fail_cdc
```

The task responsibilities are:

* `start_cdc` — verifies checkpoint readiness and starts the ETL run and CDC batch.
* `extract_cdc` — reads committed MySQL binlog transactions and persists RAW events.
* `transform_cdc` — validates RAW event semantics and extracts primary keys.
* `load_cdc` — persists final CDC events and advances the APPLY checkpoint.
* `complete_cdc` — completes CDC batch metrics and marks the ETL run successful.
* `fail_cdc` — marks the corresponding ETL audit run failed when an upstream stage fails.

The DAG delegates CDC processing logic to:

```text
02-etl-pipelines/src/cdc/stream.py
02-etl-pipelines/src/cdc/extract.py
02-etl-pipelines/src/cdc/transform.py
02-etl-pipelines/src/cdc/apply.py
02-etl-pipelines/src/cdc/pipeline.py
```

Airflow therefore coordinates the CDC pipeline without embedding CDC business logic in the DAG itself.

The DAG uses:

```text
max_active_runs = 1
```

to prevent overlapping CDC consumers from independently reading or advancing the same logical binlog stream.

---

### ecommerce_audit_reconciliation

Maintenance DAG used to reconcile stale ETL audit runs.

```text
Schedule: 30 2 * * *
Timezone: America/Lima
Max active runs: 1
```

The DAG runs every day at 02:30 Lima time.

It searches for ETL executions that:

```text
status = RUNNING
```

and have remained in that state for more than 15 minutes.

Those executions are marked as:

```text
FAILED
```

with the error message:

```text
Marked as stale: execution ended without final audit status
```

This prevents abandoned ETL executions from remaining permanently marked as
running.

---

### ecommerce_connectivity_check

Utility DAG used to validate database connectivity.

It verifies that Airflow can connect to:

- MySQL using `mysql_source`.
- PostgreSQL using `postgres_dw`.

This DAG has no automatic schedule and can be triggered manually.

---

## Scheduling

The production schedules are:

| DAG | Schedule | Purpose |
| --- | --- | --- |
| `ecommerce_change_data_capture` | every 5 minutes | Continuous batch-oriented CDC |
| `ecommerce_audit_reconciliation` | `30 2 * * *` | Daily stale-run reconciliation |

`ecommerce_audit_reconciliation` uses the `America/Lima` timezone.

The CDC DAG uses `max_active_runs=1` so that only one consumer execution
can own ordered binlog processing at a time.

## Retry and Timeout Policy

The audit reconciliation task uses:

```text
Retries: 1
Retry delay: 30 seconds
Execution timeout: 2 minutes
```

The CDC DAG uses stage-specific retry policies:

```text
start_cdc
Retries: 0

extract_cdc
Retries: 2
Retry delay: 30 seconds
Execution timeout: 5 minutes

transform_cdc
Retries: 2

load_cdc
Retries: 2

complete_cdc
Retries: 2

fail_cdc
Retries: 0
```

CDC retries are safe because durable event persistence is idempotent,
READ/APPLY progression is explicitly controlled, and committed LOAD
replay avoids reapplying Data Warehouse effects.

## ETL Audit Correlation

Airflow execution metadata is stored in:

```text
audit.etl_run
```

The ETL audit table contains the following orchestration metadata:

```text
orchestrator
orchestrator_run_id
orchestrator_task_id
orchestrator_try_number
```

For Airflow executions:

```text
orchestrator = airflow
```

This allows an ETL execution in PostgreSQL to be correlated with the corresponding Airflow DAG run and task attempt.

Example:

```text
Airflow DAG Run
      |
      | run_id
      | task_id
      | try_number
      v
audit.etl_run
```

This improves pipeline observability and troubleshooting.

---



## Stale Run Reconciliation

The `ecommerce_audit_reconciliation` DAG detects CDC audit executions
that remain in `RUNNING` state beyond the configured threshold.

It reconciles:

```text
pipeline_name = change_data_capture
```

Stale-run reconciliation is a maintenance safeguard for executions that
terminate without reaching normal success or failure finalization.

It does not move READ or APPLY checkpoints and does not mutate CDC event
payloads or Data Warehouse state.

## CDC Checkpoint and Failure Model

The CDC pipeline tracks two durable MySQL binlog checkpoints in:

```text
audit.cdc_checkpoint
```

The checkpoints are:

```text
READ  = mysql_sales_binlog_read
APPLY = mysql_sales_binlog
```

READ represents the source position durably persisted into:

```text
cdc.raw_change_event
```

APPLY represents the source position durably persisted into:

```text
cdc.change_event
```

A new CDC run is allowed to start only when:

```text
READ == APPLY
```

If:

```text
READ > APPLY
```

then durable staged work exists and has not yet reached the final CDC layer.

In that condition the pipeline fails fast instead of starting a new extraction batch.

The current recovery model retries or clears the failed tasks belonging to the same Airflow DagRun so processing resumes from durable RAW or TRANSFORMED staging.

The checkpoint model therefore prevents staged CDC work from being silently skipped.

### Atomic CDC Boundaries

The EXTRACT stage commits:

```text
RAW events
+
READ checkpoint
```

inside one PostgreSQL transaction.

The LOAD stage commits:

```text
FINAL events
+
APPLY checkpoint
```

inside one PostgreSQL transaction.

This keeps durable data and checkpoint state consistent during failures and retries.

### Known Hardening Area

If LOAD succeeds and advances APPLY, but `complete_cdc` fails afterward, READ and APPLY may already be equal even though the audit run is incomplete.

A real validation run exposed this condition while preserving the CDC data correctly.

A stronger unfinished-run or unfinished-batch readiness check is reserved for later pipeline hardening.

---

## Validation

Validate the Docker Compose configuration:

```bash
docker compose config --quiet
```

Check running containers:

```bash
docker compose ps
```

Check Airflow DAG import errors:

```bash
docker exec airflow-scheduler \
  airflow dags list-import-errors --local
```

Expected result:

```text
No data found
```

List locally parsed DAGs:

```bash
docker exec airflow-scheduler \
  airflow dags list --local
```

Validate the CDC DAG:

```bash
docker exec airflow-scheduler \
  airflow dags details \
  ecommerce_change_data_capture \
  --output table
```

The validated CDC DAG state includes:

```text
has_import_errors = False
max_active_runs   = 1
is_paused         = False
```

Run the complete ETL and CDC test suite:

```bash
PYTHONPATH=02-etl-pipelines/src \
pytest -q 02-etl-pipelines/tests
```

Validated result:

```text
125 passed
```

The CDC orchestration has also been validated end to end with one committed MySQL transaction containing:

```text
1 INSERT
1 UPDATE
1 DELETE
```

The successful CDC execution produced:

```text
RAW          = 3
TRANSFORMED  = 3
FINAL        = 3
```

The same execution crossed a real MySQL binlog rotation and completed with:

```text
READ == APPLY
```

A subsequent no-op CDC execution completed successfully with zero transactions and zero events while preserving checkpoint equality.

For the detailed CDC design and validation evidence, see:

[`../docs/cdc-design.md`](../docs/cdc-design.md)

---

## Local Access

The Airflow web interface is exposed through the port configured by:

```text
AIRFLOW_PORT
```

The default development configuration uses:

```text
8080
```

The local Airflow interface is therefore available at:


```text
http://localhost:8080
```

---

## Logs

Airflow runtime logs are stored locally in:

```text
airflow/logs/
```

Runtime logs are excluded from Git because they are generated by Airflow and
are not part of the project source code.
