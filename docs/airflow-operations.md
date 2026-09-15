# Airflow CDC Operations

This runbook describes how to execute the CDC pipeline manually through Apache Airflow and how to configure or modify its automatic schedule.

It assumes that the infrastructure, Full Load, Airflow connections, and CDC bootstrap have already been validated.

---

## Part A — Manual CDC Execution

### 1. Verify the Services

From the repository root:

```bash
docker compose ps
```

The main persistent services should be active:

```text
ecommerce-mysql
ecommerce-postgres
airflow-postgres
airflow-api-server
airflow-scheduler
airflow-dag-processor
```

`airflow-init` may show:

```text
Exited (0)
```

which is expected.

---

### 2. Verify the CDC DAG

```bash
docker compose exec -T airflow-scheduler \
  airflow dags list \
  -l \
  -B dags-folder
```

Confirm that this DAG exists:

```text
ecommerce_change_data_capture
```

Its execution flow is:

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

---

### 3. Check the Paused State

```bash
docker compose exec -T airflow-scheduler \
  airflow dags list \
  -l \
  -B dags-folder \
  | grep '^ecommerce_change_data_capture'
```

Interpretation:

```text
True
    DAG paused

False
    DAG enabled
```

A manual DagRun created while the DAG is paused may remain queued.

---

### 4. Enable the DAG

If necessary:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags unpause \
  ecommerce_change_data_capture
```

Verify again:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags list \
  -l \
  -B dags-folder \
  | grep '^ecommerce_change_data_capture'
```

The paused field should show:

```text
False
```

Always verify the resulting DAG state instead of relying only on the direct `pause` or `unpause` output.

---

### 5. Trigger a Manual DagRun

Create a real Airflow-managed execution:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags trigger \
  ecommerce_change_data_capture
```

A normal lifecycle is:

```text
queued
    ↓
running
    ↓
success
```

If a pipeline stage fails, the DagRun should finish as:

```text
failed
```

---

### 6. Monitor the DagRun

List recent runs:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags list-runs \
  ecommerce_change_data_capture \
  -o table
```

Filter successful runs:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags list-runs \
  ecommerce_change_data_capture \
  --state success \
  -o table
```

Filter failed runs:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags list-runs \
  ecommerce_change_data_capture \
  --state failed \
  -o table
```

---

### 7. Validate the Run

Check the ETL audit:

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
WHERE pipeline_name = 'change_data_capture'
ORDER BY run_id DESC
LIMIT 5;
SQL
```

Successful execution:

```text
status = SUCCESS
```

Inspect the CDC batch:

```bash
docker compose exec -T postgres \
  sh -lc 'psql \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -P pager=off' <<'SQL'
SELECT
    batch_id,
    run_id,
    start_binlog_file,
    start_binlog_position,
    end_binlog_file,
    end_binlog_position,
    transactions_processed,
    events_processed,
    insert_events,
    update_events,
    delete_events
FROM audit.cdc_batch
ORDER BY batch_id DESC
LIMIT 5;
SQL
```

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

After a successful execution:

```text
READ == APPLY
```

The CDC event-layer counts can also be inspected:

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

These counts are cumulative.

---

### 8. Controlled DAG Testing

For controlled validation:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags test \
  ecommerce_change_data_capture \
  -B dags-folder
```

Distinction:

```text
airflow dags test
    controlled validation

airflow dags trigger
    real Airflow-managed DagRun
```

Use `dags trigger` when validating normal scheduler-managed operation.

---

## Part B — Automatic Scheduling

### 9. Current Schedule

The DAG is defined in:

```text
airflow/dags/ecommerce_change_data_capture.py
```

Current configuration:

```python
schedule="*/5 * * * *"
```

which means:

```text
every 5 minutes
```

The DAG also uses:

```text
Timezone        America/Lima
catchup         False
max_active_runs 1
```

---

### 10. Cron Format

Airflow accepts standard five-field cron expressions:

```text
minute hour day_of_month month day_of_week
```

Example:

```text
30 2 * * *
```

means:

```text
every day at 02:30
```

The CDC DAG interprets the expression using:

```text
America/Lima
```

---

### 11. Configure a Specific Schedule

To run every day at 02:30 Lima time, change:

```python
schedule="*/5 * * * *",
```

to:

```python
schedule="30 2 * * *",
```

Common examples:

```text
30 2 * * *     daily at 02:30
0 6 * * *      daily at 06:00
0 18 * * *     daily at 18:00
0 * * * *      every hour
0 8 * * 1-5    Monday-Friday at 08:00
0 9 * * 1      every Monday at 09:00
*/15 * * * *   every 15 minutes
*/5 * * * *    every 5 minutes
```

Because the DAG directory is mounted into the Airflow containers, changing only the DAG file does not require rebuilding the image.

---

### 12. Verify the Updated DAG

After saving the file:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags list \
  -l \
  -B dags-folder \
  | grep '^ecommerce_change_data_capture'
```

The DAG should remain visible and parse successfully.

Inspect the next calculated execution:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags next-execution \
  ecommerce_change_data_capture
```

Confirm that the result matches the intended schedule.

---

### 13. Enable Automatic Scheduling

If the DAG is paused:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags unpause \
  ecommerce_change_data_capture
```

Verify:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags list \
  -l \
  -B dags-folder \
  | grep '^ecommerce_change_data_capture'
```

Expected:

```text
paused = False
```

Airflow will now create scheduled DagRuns automatically.

---

## Part C — Schedule Management

### 14. Pause Scheduling

To prevent new scheduled runs:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags pause \
  ecommerce_change_data_capture
```

Verify:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags list \
  -l \
  -B dags-folder \
  | grep '^ecommerce_change_data_capture'
```

Expected:

```text
paused = True
```

Pausing does not remove:

```text
CDC checkpoints
CDC event history
Data Warehouse state
audit history
```

---

### 15. Change the Schedule Safely

Recommended procedure:

```text
1. Pause the DAG.
2. Edit the schedule.
3. Save the DAG file.
4. Verify DAG parsing.
5. Check the next execution.
6. Unpause the DAG.
```

Example:

```python
schedule="0 1 * * *"
```

means:

```text
daily at 01:00 America/Lima
```

---

### 16. Disable Automatic Scheduling

For manual-only operation:

```python
schedule=None
```

Airflow will stop creating periodic runs.

Manual execution remains available:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags trigger \
  ecommerce_change_data_capture
```

---

## Part D — Operational Notes

### 17. Logical Date Is Not a Timer

Airflow's logical date represents the temporal context or data interval associated with a DagRun.

It does not mean:

```text
wait until this time and execute
```

Do not use a future `--logical-date` as a replacement for scheduling.

For recurring execution, configure:

```python
schedule="..."
```

using the required cron expression.

---

### 18. Recommended Operating Flows

Manual execution:

```text
verify services
    ↓
verify DAG state
    ↓
unpause if necessary
    ↓
airflow dags trigger
    ↓
monitor DagRun
    ↓
validate audit and CDC batch
    ↓
validate READ == APPLY
```

Scheduled execution:

```text
pause DAG
    ↓
configure schedule
    ↓
verify DAG parsing
    ↓
inspect next execution
    ↓
unpause DAG
    ↓
scheduler owns future runs
```

Recommended demonstration modes:

```text
Low-latency CDC
    schedule="*/5 * * * *"

Daily batch-style CDC
    schedule="30 2 * * *"

Manual only
    schedule=None
```

The final operational flow remains:

```text
MySQL changes
    ↓
MySQL binary log
    ↓
Airflow
    ↓
CDC EXTRACT
    ↓
RAW
    ↓
CDC TRANSFORM
    ↓
TRANSFORMED
    ↓
CDC LOAD / APPLY
    ├── FINAL
    ├── Data Warehouse
    └── APPLY checkpoint
```

A successful CDC execution should finish with consistent durable state and converged READ/APPLY checkpoints.
