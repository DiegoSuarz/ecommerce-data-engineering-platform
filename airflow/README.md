# Airflow Orchestration

## Overview

Apache Airflow is used to orchestrate the incremental ETL pipeline of the
E-Commerce Data Engineering Platform.

The orchestration layer is responsible for:

- Scheduling the incremental ETL pipeline.
- Managing task retries and execution timeouts.
- Connecting Airflow with the MySQL source database.
- Connecting Airflow with the PostgreSQL data warehouse.
- Recording Airflow execution metadata in the ETL audit table.
- Reconciling ETL runs that remain incorrectly marked as `RUNNING`.

Airflow runs locally using Docker Compose.

---

## Architecture

The orchestration environment contains the following main components:

```text
                    Apache Airflow
                          |
             +------------+------------+
             |                         |
             v                         v
        MySQL OLTP               PostgreSQL DW
       mysql_source              postgres_dw
             |                         |
             +-----------+-------------+
                         |
                         v
                 Incremental ETL
                         |
                         v
                  audit.etl_run
```

Airflow uses a separate PostgreSQL database for its internal metadata.

The Airflow metadata database is independent from the PostgreSQL data
warehouse used by the ETL pipeline.

---

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
AIRFLOW_IMAGE_NAME
AIRFLOW_UID

AIRFLOW_POSTGRES_DATABASE
AIRFLOW_POSTGRES_USER
AIRFLOW_POSTGRES_PASSWORD

AIRFLOW_PORT
AIRFLOW_JWT_SECRET
AIRFLOW_FERNET_KEY
```

Real credentials and secrets must be stored in the local `.env` file.

The `.env` file must never be committed to Git.

---

## Airflow Connections

The DAGs use Airflow Connections instead of hardcoded database credentials.

Two database connections are required.

### MySQL source

```text
Connection ID: mysql_source
```

This connection points to the MySQL OLTP database containing the source
`orders` table.

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

The project currently contains four Airflow DAGs.

### ecommerce_incremental_load

Main production ETL orchestration DAG.

```text
Schedule: 0 2 * * *
Timezone: America/Lima
Max active runs: 1
```

The DAG runs the incremental ETL pipeline every day at 02:00 Lima time.

The task executes:

```text
run_incremental_pipeline
```

The task loads the existing ETL implementation from:

```text
02-etl-pipelines/src/incremental_load.py
```

Airflow therefore orchestrates the ETL without duplicating the ETL business
logic inside the DAG.

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

### ecommerce_smoke_test

Simple Airflow validation DAG.

It verifies that the Airflow orchestration environment can successfully
execute a task.

This DAG has no automatic schedule.

---

## Scheduling

The production schedules are:

| DAG | Schedule | Lima time |
|---|---|---|
| `ecommerce_incremental_load` | `0 2 * * *` | 02:00 |
| `ecommerce_audit_reconciliation` | `30 2 * * *` | 02:30 |

Both scheduled DAGs use:

```text
America/Lima
```

as their timezone.

The reconciliation DAG runs after the incremental ETL schedule so it can
detect ETL executions that did not reach a final audit state.

---

## Retry and Timeout Policy

The incremental ETL task uses:

```text
Retries: 2
Retry delay: 30 seconds
Execution timeout: 5 minutes
```

The audit reconciliation task uses:

```text
Retries: 1
Retry delay: 30 seconds
Execution timeout: 2 minutes
```

Retries allow Airflow to recover from temporary failures.

Execution timeouts prevent tasks from running indefinitely.

---

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

This allows an ETL execution in PostgreSQL to be correlated with the
corresponding Airflow DAG run and task attempt.

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

## Incremental Loading

The ETL pipeline uses a composite watermark.

The watermark is stored in:

```text
audit.pipeline_watermark
```

The current watermark strategy uses:

```text
(updated_at, order_id)
```

This provides a deterministic tie-breaker when multiple source rows have the
same `updated_at` timestamp.

The watermark is updated only after the incremental pipeline completes
successfully.

If the pipeline fails, the previous watermark remains unchanged.

This allows the failed batch to be processed again during the next execution.

---

## Stale Run Reconciliation

An ETL execution normally follows this lifecycle:

```text
RUNNING
   |
   +------> SUCCESS
   |
   +------> FAILED
```

Unexpected interruptions can leave an audit record in:

```text
RUNNING
```

even when the execution no longer exists.

The reconciliation process detects these stale records and changes them to:

```text
FAILED
```

The current stale threshold is:

```text
15 minutes
```

The reconciliation logic is implemented in:

```text
02-etl-pipelines/src/audit.py
```

through:

```text
mark_stale_etl_runs()
```

Integration tests validate that the function:

- Marks stale `RUNNING` executions as `FAILED`.
- Does not modify recent `RUNNING` executions.
- Does not modify executions from another pipeline.
- Returns the IDs of the audit runs that were updated.

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
docker compose exec airflow-dag-processor \
  airflow dags list-import-errors
```

Expected result:

```text
No data found
```

List locally parsed DAGs:

```bash
docker compose exec airflow-scheduler \
  airflow dags list --local
```

Run the ETL test suite:

```bash
pytest -v 02-etl-pipelines/tests
```

The current test suite contains integration and unit tests for:

- Incremental extraction.
- Composite watermarks.
- Incremental load behavior.
- Date dimension transformations.
- ETL audit reconciliation.

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


```markdown
```text
http://localhost:8080
```
```

---

## Logs

Airflow runtime logs are stored locally in:

```text
airflow/logs/
```

Runtime logs are excluded from Git because they are generated by Airflow and
are not part of the project source code.
