# Local Reproduction and Platform Startup

This runbook describes how to reproduce the E-Commerce Data Engineering Platform from a clean local environment, execute the initial Full Load, validate the Data Warehouse, initialize CDC safely, and enable Airflow scheduling.

The expected sequence is:

```text
Clone repository
    ↓
Configure environment
    ↓
Create Python environment
    ↓
Start Docker infrastructure
    ↓
Validate databases and Airflow
    ↓
Execute Full Load
    ↓
Validate Data Warehouse
    ↓
Execute first controlled CDC run
    ↓
Validate CDC state
    ↓
Enable scheduled processing
```

---

## 1. Prerequisites

Required software:

* Git
* Docker
* Docker Compose
* Python 3.10

Verify:

```bash
git --version
docker --version
docker compose version
python3 --version
```

---

## 2. Clone the Repository

```bash
git clone https://github.com/DiegoSuarz/ecommerce-data-engineering-platform.git
cd ecommerce-data-engineering-platform
```

Verify the branch:

```bash
git branch --show-current
```

Expected:

```text
main
```

To reproduce the stable M9 release exactly:

```bash
git checkout v0.7.0
```

Otherwise remain on `main`.

---

## 3. Configure the Environment

Create the local environment file:

```bash
cp .env.example .env
```

Edit `.env` and provide values for all required passwords and secrets.

The main configuration groups are:

```text
MySQL
MySQL CDC
PostgreSQL Data Warehouse
Airflow
Airflow metadata PostgreSQL
```

The `.env` file must remain local and must never be committed to Git.

---

## 4. Generate Airflow Secrets

Generate the JWT secret:

```bash
python3 - <<'PY'
import secrets
print(secrets.token_urlsafe(32))
PY
```

Generate the Fernet key:

```bash
python3 - <<'PY'
import base64
import os

print(
    base64.urlsafe_b64encode(
        os.urandom(32)
    ).decode()
)
PY
```

Copy the values into:

```text
AIRFLOW_JWT_SECRET=
AIRFLOW_FERNET_KEY=
```

inside `.env`.

---

## 5. Create the Python Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Verify:

```bash
python --version
```

The project uses Python 3.10.

---

## 6. Start the Docker Infrastructure

From the repository root:

```bash
docker compose up -d
```

Inspect all containers:

```bash
docker compose ps -a
```

The main services are:

```text
ecommerce-mysql
ecommerce-postgres
airflow-postgres
airflow-api-server
airflow-scheduler
airflow-dag-processor
```

`airflow-init` is expected to finish with:

```text
Exited (0)
```

because it performs initialization and then exits.

---

## 7. Validate Automatic Initialization

Inspect the Airflow initialization logs:

```bash
docker compose logs --no-color airflow-init
```

On a fresh installation:

```text
MySQL
    executes 01-oltp-database/sql/

PostgreSQL
    executes 03-data-warehouse/sql/

Airflow
    runs airflow db migrate
    runs airflow/scripts/bootstrap_connections.py
```

The Airflow connection bootstrap should create or update:

```text
mysql_source
mysql_cdc_source
postgres_dw
```

No credentials should appear in the logs.

---

## 8. Validate the MySQL Source

Query the source counts:

```bash
docker compose exec -T mysql \
  sh -lc 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql \
    --protocol=socket \
    --user=root \
    --database="$MYSQL_DATABASE" \
    --batch \
    --skip-column-names' <<'SQL'
SELECT 'categories', COUNT(*)
FROM categories

UNION ALL

SELECT 'countries', COUNT(*)
FROM countries

UNION ALL

SELECT 'orders', COUNT(*)
FROM orders;
SQL
```

Fresh `v0.7.0` baseline:

```text
categories    5
countries     56
orders        300000
```

---

## 9. Validate the PostgreSQL Structure

Verify the main relations:

```bash
docker compose exec -T postgres \
  sh -lc 'psql \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -At' <<'SQL'
SELECT to_regclass('staging.orders');
SELECT to_regclass('dw.dim_date');
SELECT to_regclass('dw.dim_category');
SELECT to_regclass('dw.dim_country');
SELECT to_regclass('dw.fact_sales');
SELECT to_regclass('audit.etl_run');
SELECT to_regclass('audit.cdc_checkpoint');
SELECT to_regclass('audit.cdc_batch');
SELECT to_regclass('cdc.raw_change_event');
SELECT to_regclass('cdc.transformed_event');
SELECT to_regclass('cdc.change_event');
SQL
```

Each query should return the corresponding relation name.

Before the Full Load, staging and warehouse tables should still be empty.

---

## 10. Execute the Initial Full Load

The Full Load owns:

```text
initial warehouse bootstrap
complete warehouse reconstruction
```

Run:

```bash
python 02-etl-pipelines/src/full_load.py
```

The pipeline performs:

```text
source extraction
    ↓
staging load
    ↓
quality checks
    ↓
dimension construction
    ↓
temporal surrogate-key resolution
    ↓
fact loading
    ↓
reconciliation
    ↓
audit completion
```

Expected final log:

```text
Full load completed successfully
```

---

## 11. Validate the Full Load

Query the main row counts:

```bash
docker compose exec -T postgres \
  sh -lc 'psql \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -At' <<'SQL'
SELECT 'staging.categories', COUNT(*)
FROM staging.categories

UNION ALL

SELECT 'staging.countries', COUNT(*)
FROM staging.countries

UNION ALL

SELECT 'staging.orders', COUNT(*)
FROM staging.orders

UNION ALL

SELECT 'dw.dim_category', COUNT(*)
FROM dw.dim_category

UNION ALL

SELECT 'dw.dim_country', COUNT(*)
FROM dw.dim_country

UNION ALL

SELECT 'dw.dim_date', COUNT(*)
FROM dw.dim_date

UNION ALL

SELECT 'dw.fact_sales', COUNT(*)
FROM dw.fact_sales;
SQL
```

Fresh baseline:

```text
staging.categories    5
staging.countries     56
staging.orders        300000

dw.dim_category       5
dw.dim_country        56
dw.dim_date           1096
dw.fact_sales         300000
```

Verify the latest audit records:

```bash
docker compose exec -T postgres \
  sh -lc 'psql \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -P pager=off' <<'SQL'
SELECT
    run_id,
    pipeline_name,
    status,
    rows_extracted,
    rows_loaded,
    rows_rejected,
    started_at,
    finished_at
FROM audit.etl_run
ORDER BY run_id DESC
LIMIT 5;
SQL
```

The Full Load must finish with:

```text
pipeline_name = full_load
status        = SUCCESS
```

---

## 12. Verify Airflow Connections

Query only the connection IDs:

```bash
docker compose exec -T airflow-postgres \
  sh -lc 'psql \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -At' <<'SQL'
SELECT conn_id
FROM connection
WHERE conn_id IN (
    'mysql_source',
    'mysql_cdc_source',
    'postgres_dw'
)
ORDER BY conn_id;
SQL
```

Expected:

```text
mysql_cdc_source
mysql_source
postgres_dw
```

These connections are created automatically during `airflow-init`.

---

## 13. Verify the DAGs

```bash
docker compose exec -T airflow-scheduler \
  airflow dags list \
  -l \
  -B dags-folder
```

Expected DAGs:

```text
ecommerce_change_data_capture
ecommerce_audit_reconciliation
ecommerce_connectivity_check
```

Fresh DAGs are created paused intentionally.

---

## 14. Validate Airflow Connectivity

Run the diagnostic DAG:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags test \
  ecommerce_connectivity_check \
  -B dags-folder
```

This validates:

```text
mysql_source
    → MySQL

postgres_dw
    → PostgreSQL
```

Both connectivity tasks must succeed before CDC scheduling is enabled.

---

## 15. Execute the First Controlled CDC Run

Keep the CDC DAG paused and execute:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags test \
  ecommerce_change_data_capture \
  -B dags-folder
```

This validates:

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

For a fresh installation with no new source mutations, this will normally be a zero-event execution.

It also initializes the CDC checkpoints safely.

`fail_cdc` should be skipped on a successful run.

---

## 16. Validate CDC State

Inspect the checkpoints:

```bash
docker compose exec -T postgres \
  sh -lc 'psql \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -At' <<'SQL'
SELECT
    checkpoint_name,
    binlog_file,
    binlog_position
FROM audit.cdc_checkpoint
WHERE pipeline_name = 'change_data_capture'
ORDER BY checkpoint_name;
SQL
```

Expected checkpoint names:

```text
mysql_sales_binlog
mysql_sales_binlog_read
```

Binlog coordinates depend on the local MySQL instance.

The important post-run condition is:

```text
READ == APPLY
```

Inspect the event layers:

```bash
docker compose exec -T postgres \
  sh -lc 'psql \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -At' <<'SQL'
SELECT 'raw', COUNT(*)
FROM cdc.raw_change_event

UNION ALL

SELECT 'transformed', COUNT(*)
FROM cdc.transformed_event

UNION ALL

SELECT 'final', COUNT(*)
FROM cdc.change_event;
SQL
```

If no source changes occurred after Full Load, the initial result will normally be:

```text
raw           0
transformed   0
final         0
```

---

## 17. Enable Continuous CDC Scheduling

Unpause the CDC DAG:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags unpause \
  ecommerce_change_data_capture
```

Verify the actual state:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags list \
  -l \
  -B dags-folder \
  | grep '^ecommerce_change_data_capture'
```

The paused field should be:

```text
False
```

The default CDC schedule is every five minutes.

The DAG uses:

```text
max_active_runs = 1
```

to prevent overlapping CDC consumers.

---

## 18. Enable Audit Reconciliation

Enable the maintenance DAG:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags unpause \
  ecommerce_audit_reconciliation
```

Verify:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags list \
  -l \
  -B dags-folder \
  | grep '^ecommerce_audit_reconciliation'
```

This DAG reconciles stale ETL audit executions.

`ecommerce_connectivity_check` can remain paused because it is intended for manual diagnostics.

---

## 19. Verify the Final Operational State

Check the services:

```bash
docker compose ps
```

Check the DAGs:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags list \
  -l \
  -B dags-folder
```

Expected operational state:

```text
ecommerce_change_data_capture
    paused = False

ecommerce_audit_reconciliation
    paused = False

ecommerce_connectivity_check
    manual diagnostic DAG
```

With the default configuration, the Airflow interface is available at:

```text
http://localhost:8080
```

or the port configured through:

```text
AIRFLOW_PORT
```

The final processing responsibility is:

```text
Full Load
    → bootstrap / reconstruction

CDC + Airflow
    → continuous synchronization
```

After bootstrap, ongoing changes to:

```text
categories
countries
orders
```

are processed through the MySQL binary log rather than through recurring Full Loads.
