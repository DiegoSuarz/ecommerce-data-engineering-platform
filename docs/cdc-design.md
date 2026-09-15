# Change Data Capture Design

## 1. Purpose

This document describes the Change Data Capture (CDC) architecture implemented in the E-Commerce Data Engineering Platform.

The platform uses Full Load for warehouse bootstrap or reconstruction and CDC as the continuous synchronization mechanism. CDC captures committed row-level changes directly from the MySQL binary log.

The implementation captures:

* INSERT operations;
* UPDATE operations;
* DELETE operations;
* transaction boundaries;
* source binlog coordinates;
* event timestamps;
* durable processing state.

The CDC pipeline is designed as a batch-oriented, log-based ingestion process orchestrated by Apache Airflow.

---

## 2. Why Log-Based CDC

Continuous warehouse synchronization requires observing the actual source
changes, including physical DELETE operations.

CDC reads committed row events directly from the MySQL binary log, so the
pipeline receives INSERT, UPDATE, and DELETE events rather than inferring
changes by repeatedly querying the latest source-table state.

This makes the binary log the authoritative ordered change stream after
the initial Full Load bootstrap.

## 3. MySQL Binary Log Configuration

The MySQL source is configured for row-based binary logging.

The relevant settings are:

```text
server_id = 1
log_bin = ON
binlog_format = ROW
binlog_row_image = FULL
binlog_row_metadata = FULL
```

`ROW` format is required because the CDC reader consumes row-level write, update, and delete events.

`FULL` row images provide complete before and after values when available, allowing the pipeline to reconstruct event semantics without querying the current table state.

Binary logs are retained for a finite period so that CDC consumers can resume from previously stored coordinates.

---

## 4. CDC Source User

CDC uses a dedicated MySQL account instead of the application or root database user.

The account is provisioned by:

```text
01-oltp-database/sql/007_configure_cdc_user.sh
```

Its responsibilities are limited to:

* reading the required `sales` source tables;
* reading the MySQL replication stream.

The account does not have privileges to mutate source data.

This follows the principle of least privilege and isolates CDC access from normal application credentials.

---

## 5. Source Scope

The current CDC implementation monitors the following tables in the `sales` schema:

```text
categories
countries
orders
```

The reader consumes the following MySQL event types:

```text
WriteRowsEvent
UpdateRowsEvent
DeleteRowsEvent
QueryEvent
XidEvent
RotateEvent
```

Only committed row changes are propagated to downstream stages.

---

## 6. Event Model

Each captured row change receives a deterministic event key based on its source binlog location:

```text
binlog_file:event_end_position:row_index
```

Example:

```text
binlog.000033:1089:0
```

This key identifies a specific row event within the binary log and is persisted with a unique constraint.

The event model includes:

* operation;
* source schema;
* source table;
* primary key;
* before values;
* after values;
* binlog file;
* event end position;
* row index;
* transaction identifier;
* commit position;
* event timestamp;
* commit timestamp.

The deterministic event key is a core part of CDC idempotency.

---

## 7. Transaction Boundaries

CDC processing is transaction-aware.

Row events are first buffered in memory until MySQL emits the transaction commit boundary.

A transaction is considered safe for downstream persistence only after its corresponding `XidEvent` is observed.

The pipeline therefore does not persist partially observed source transactions as committed CDC work.

Conceptually:

```text
BEGIN
  row event
  row event
  row event
COMMIT
   ↓
CDC transaction becomes eligible for persistence
```

This preserves source transaction boundaries during ingestion.

---

## 8. Durable Multi-Stage Architecture

CDC separates extraction, transformation, and application into durable
Airflow stages.

```text
MySQL binary log
        │
        ▼
     EXTRACT
        │
        ├── persist RAW
        └── advance READ
        │
        ▼
cdc.raw_change_event
        │
        ▼
    TRANSFORM
        │
        ▼
cdc.transformed_event
        │
        ▼
      LOAD
        │
        ├── persist cdc.change_event
        ├── apply event effects to the DW
        └── advance APPLY
```

RAW persistence and READ advancement form one PostgreSQL transaction.

FINAL event persistence, Data Warehouse mutation, and APPLY advancement
form another PostgreSQL transaction.

Event payloads therefore do not depend on Airflow XCom for durability.
XCom carries only small orchestration metadata such as run IDs, batch IDs,
coordinates, and stage counts.

## 9. RAW Layer

The EXTRACT stage writes committed source events to:

```text
cdc.raw_change_event
```

RAW represents the closest durable representation of the source binlog event.

It stores:

* operation;
* source schema and table;
* before and after row images;
* binlog coordinates;
* row index;
* source transaction metadata;
* event and commit timestamps.

The RAW layer intentionally does not require a primary key value or transformed operation semantics.

Those responsibilities belong to the TRANSFORM stage.

---

## 10. TRANSFORMED Layer

The TRANSFORM stage reads RAW events for a specific CDC batch and writes validated events to:

```text
cdc.transformed_event
```

Transformation responsibilities include:

* validating INSERT / UPDATE / DELETE row-image semantics;
* resolving the configured primary-key columns for the source table;
* extracting the event primary key;
* producing the normalized event representation used by LOAD.

Expected image semantics are:

```text
INSERT
before = NULL
after  = row

UPDATE
before = row
after  = row

DELETE
before = row
after  = NULL
```

Each transformed event references its originating RAW event.

This creates explicit lineage:

```text
RAW event
   ↓
TRANSFORMED event
   ↓
FINAL change event
```

---

## 11. FINAL CDC Layer

The LOAD stage persists validated events into:

```text
cdc.change_event
```

This table is the final durable CDC event store.

The final event includes the extracted primary key together with the complete CDC metadata required for downstream consumers.

The unique `event_key` constraint makes final persistence idempotent.

Repeated attempts to insert an event that has already been persisted do not create duplicate CDC records.

---

## 12. CDC Batch and Run Audit

Each CDC execution is associated with the normal ETL audit model.

The main audit entities are:

```text
audit.etl_run
audit.cdc_batch
audit.cdc_checkpoint
```

`audit.etl_run` records the logical pipeline execution.

`audit.cdc_batch` records CDC-specific metrics including:

* starting binlog coordinate;
* ending binlog coordinate;
* transactions processed;
* events processed;
* INSERT event count;
* UPDATE event count;
* DELETE event count.

This connects Airflow execution state with durable CDC processing metadata.

---

## 13. READ and APPLY Checkpoints

The multi-stage CDC pipeline uses two independent checkpoints.

```text
READ checkpoint
mysql_sales_binlog_read

APPLY checkpoint
mysql_sales_binlog
```

They represent different guarantees.

### READ checkpoint

The READ checkpoint identifies the MySQL binlog position that has been durably captured into RAW staging.

It is advanced atomically with RAW transaction persistence.

Conceptually:

```text
persist RAW events
      +
advance READ checkpoint
      ↓
single PostgreSQL transaction
```

### APPLY checkpoint

The APPLY checkpoint identifies the source position whose transformed events have been durably loaded into the final CDC table.

It is advanced atomically with the final LOAD.

Conceptually:

```text
persist FINAL events
      +
advance APPLY checkpoint
      ↓
single PostgreSQL transaction
```

This distinction allows extraction and application progress to be represented independently.

---

## 14. Pipeline Readiness Invariant

A new CDC batch is allowed to begin only when:

```text
READ == APPLY
```

If:

```text
READ > APPLY
```

then source data has already been captured into durable staging but has not yet been fully applied.

The pipeline fails fast instead of starting another extraction batch.

This prevents new MySQL reads from silently abandoning previously staged work.

Recovery retries or clears the failed tasks belonging to the same Airflow
DagRun so processing resumes from durable RAW or TRANSFORMED staging.

Before a new batch can begin, readiness validation requires aligned READ/APPLY
checkpoints, a successful latest CDC batch, and agreement between the latest
batch end coordinate and APPLY.

---

## 15. Checkpoint Bootstrap

CDC uses two durable checkpoints:

```text
APPLY = mysql_sales_binlog
READ  = mysql_sales_binlog_read
```

On a completely fresh CDC installation, neither checkpoint exists and there
is no CDC batch history.

In that state only, the pipeline reads the current MySQL binary-log head and
initializes APPLY at that coordinate. READ is then initialized from APPLY.

```text
current MySQL binlog head
          |
          v
        APPLY
          |
          v
         READ
```

This establishes the CDC boundary after the Full Load bootstrap so historical
source rows already represented in the analytical warehouse are not replayed
as new CDC changes.

Automatic APPLY bootstrap is deliberately fail-closed.

If APPLY is missing while READ already exists, or while previous CDC batch
history exists, the pipeline raises an error instead of silently initializing
APPLY at the current MySQL head. Moving APPLY in that state could skip durable
or previously acknowledged CDC work.

When APPLY exists but READ alone is missing, READ can safely be initialized
from APPLY.

Checkpoint initialization is idempotent and does not overwrite existing
coordinates. Once APPLY exists, normal readiness checks reuse the durable
checkpoint rather than resetting it to the current MySQL binary-log head.

---

## 16. Safe EOF Semantics

A non-blocking CDC reader can reach the end of the available MySQL binary log without encountering a relevant committed transaction.

In this situation the binlog stream itself still contains a safe read coordinate.

The pipeline captures this coordinate only after normal stream exhaustion.

The coordinate is not captured from a `finally` block.

Therefore:

```text
normal stream exhaustion
        ↓
safe EOF coordinate may advance

reader exception
        ↓
safe EOF coordinate does not advance
```

This distinction prevents failed reads from incorrectly skipping unprocessed source data.

An empty batch can therefore safely advance a checkpoint when the MySQL stream moves because of unrelated binlog activity.

---

## 17. Binlog Rotation

Checkpoint comparison supports MySQL binlog rotation.

A coordinate consists of:

```text
(binlog_file, binlog_position)
```

For example:

```text
binlog.000032:3771
        ↓
binlog.000033:1120
```

The pipeline compares the numeric binlog-file sequence before comparing positions within the same file.

This allows checkpoints to progress monotonically across log rotations.

---

## 18. Atomicity Guarantees

The CDC implementation provides two important transactional guarantees.

### EXTRACT atomicity

For each committed MySQL transaction:

```text
RAW event persistence
+
READ checkpoint advancement
```

occur in the same PostgreSQL transaction.

If either operation fails, both are rolled back.

### LOAD atomicity

For a transformed CDC batch:

```text
FINAL event persistence
+
APPLY checkpoint advancement
```

occur in the same PostgreSQL transaction.

If LOAD fails before commit, the final event state and APPLY checkpoint remain consistent.

These guarantees prevent a checkpoint from acknowledging work that was not durably persisted.

---

## 19. Idempotency

CDC persistence is designed for at-least-once execution.

Repeated processing is expected to be safe.

RAW events use a unique `event_key`.

TRANSFORMED events also preserve this event key and enforce one transformed record per RAW event.

FINAL events use the same event key with a unique constraint.

As a result:

```text
retry
   ↓
same source event
   ↓
same event_key
   ↓
no duplicate final event
```

This is especially important because Airflow may retry failed tasks.

---

## 20. Airflow Orchestration

CDC is orchestrated by:

```text
ecommerce_change_data_capture
```

The DAG uses the following task topology:

```text
start_cdc
    ↓
extract_cdc
    ↓
transform_cdc
    ↓
load_cdc
    ↓
complete_cdc
```

A failure-finalization task is also included:

```text
fail_cdc
```

It runs when an upstream CDC stage reaches a failed state and records the associated ETL run as failed.

The DAG uses:

```text
max_active_runs = 1
```

to prevent overlapping CDC consumers from independently reading and advancing the same logical checkpoint stream.

---

## 21. Failure Model

The CDC pipeline is intentionally fail-stop when staged work exists.

For example:

```text
EXTRACT succeeds
READ advances
RAW is durable

TRANSFORM or LOAD fails
APPLY remains behind
```

The resulting state is:

```text
READ > APPLY
```

A subsequent new CDC run refuses to begin.

This protects the staged batch from being silently bypassed.

Because RAW and TRANSFORMED data are stored in PostgreSQL, failed downstream tasks can be retried without requiring the original MySQL events to be re-read.

---

## 22. Completion Failure Observation

During the first multi-stage end-to-end validation, a real integration failure occurred after LOAD had completed successfully.

The data state was:

```text
RAW          persisted
TRANSFORMED  persisted
FINAL        persisted
READ         advanced
APPLY        advanced
```

The `complete_cdc` task then failed because the coordinator passed metric keyword names that did not match the existing `complete_cdc_batch()` audit interface.

The Airflow run was correctly marked failed.

Importantly:

```text
READ == APPLY
```

and the CDC data itself remained complete and consistent.

The issue was fixed by aligning the coordinator with the audit-function contract and adding a regression test that verifies the exact call interface.

This incident demonstrated that LOAD atomicity protected the CDC data even when audit finalization failed afterward.

This incident exposed a readiness gap in the original M8 design: checkpoint equality alone could not distinguish a fully finalized run from one whose LOAD commit succeeded but whose audit finalization remained incomplete.

M9 resolved this gap by extending readiness validation beyond READ/APPLY equality. The current pipeline also inspects the latest durable CDC batch state before starting a new batch and fails closed when previous CDC work is not fully finalized.

The incident is retained here as historical evidence of how the CDC readiness model evolved.

---

## 23. End-to-End Validation

A real CDC transaction was executed against the `categories` source table containing:

```text
1 INSERT
1 UPDATE
1 DELETE
```

All three source changes belonged to one MySQL transaction.

The successful multi-stage CDC execution produced:

```text
run_id   = 506
batch_id = 225

RAW          = 3
TRANSFORMED  = 3
FINAL        = 3

transactions_processed = 1
events_processed       = 3

insert_events = 1
update_events = 1
delete_events = 1
```

The batch also validated binlog rotation:

```text
start = binlog.000032:3771
end   = binlog.000033:1120
```

Both checkpoints converged to:

```text
READ  = binlog.000033:1120
APPLY = binlog.000033:1120
```

The ETL audit state completed as:

```text
SUCCESS
rows_extracted = 3
rows_loaded    = 3
rows_rejected  = 0
```

---

## 24. Empty-Batch Validation

An immediate second execution was performed without generating new relevant source changes.

The resulting CDC batch contained:

```text
run_id   = 507
batch_id = 226

transactions_processed = 0
events_processed       = 0

insert_events = 0
update_events = 0
delete_events = 0
```

The batch completed successfully with:

```text
READ  = binlog.000033:1120
APPLY = binlog.000033:1120
```

This validates successful no-op execution and checkpoint stability.

### Fresh-Install Reproducibility Validation

The final CDC-first architecture was also validated from a clean local
installation after removing the persistent Docker volumes for MySQL,
PostgreSQL, and the Airflow metadata database.

The fresh infrastructure bootstrap recreated the databases and automatically
provisioned the required Airflow connections:

```text
mysql_source
mysql_cdc_source
postgres_dw
```

The fresh MySQL source contained:

```text
categories = 5
countries  = 56
orders     = 300000
```

A Full Load then reconstructed the analytical warehouse:

```text
dim_category = 5
dim_country  = 56
dim_date     = 1096
fact_sales   = 300000
```

With no existing CDC checkpoints or batch history, CDC readiness safely
bootstrapped both durable checkpoints from the current MySQL binary-log head:

```text
READ  = binlog.000003:157
APPLY = binlog.000003:157
```

A first zero-event CDC DagRun completed successfully without changing the
warehouse or CDC event layers:

```text
transactions = 0
events       = 0

READ  = binlog.000003:157
APPLY = binlog.000003:157
```

The final mutation probe captured two complete INSERT / UPDATE / DELETE
cycles over category, country, and order records. The resulting CDC batch
processed:

```text
transactions = 6
events       = 18

INSERT = 6
UPDATE = 6
DELETE = 6

RAW         = 18
TRANSFORMED = 18
FINAL       = 18
```

The batch advanced both checkpoints atomically across the pending source
range:

```text
start = binlog.000003:157
end   = binlog.000003:5775

READ  = binlog.000003:5775
APPLY = binlog.000003:5775
```

The analytical warehouse preserved the resulting SCD history while the
final DELETE removed the probe fact:

```text
dim_category = 9
dim_country  = 60
dim_date     = 1097
fact_sales   = 300000
```

All probe dimension versions were historical after the final DELETE, and
the probe order was absent from `dw.fact_sales`.

A subsequent scheduled CDC run completed as a no-op at:

```text
binlog.000003:5775 -> binlog.000003:5775
```

This fresh-install validation demonstrates the complete recovery path from
empty infrastructure through Full Load bootstrap, CDC checkpoint bootstrap,
real Airflow execution, CDC-to-DW mutation, checkpoint convergence, and
retry-safe no-op processing.

---

## 25. Automated Validation

At the final CDC-first baseline, after repository cleanup and fresh-install hardening, the complete ETL test suite passed:

```text
122 passed
```

Additional final checks included:

```text
git diff --check
```

with no whitespace errors, and Airflow DAG validation with:

```text
has_import_errors = False
max_active_runs   = 1
```

The CDC DAG was then returned to active scheduling.

---

## 26. Current v0.7.0 Guarantees

The current CDC-first baseline provides:

```text
✓ log-based MySQL CDC
✓ INSERT capture
✓ UPDATE capture
✓ DELETE capture
✓ committed-transaction processing
✓ deterministic event identity
✓ durable RAW staging
✓ durable TRANSFORMED staging
✓ durable FINAL event persistence
✓ direct CDC-to-Data-Warehouse application
✓ READ / APPLY checkpoint separation
✓ atomic RAW + READ persistence
✓ atomic FINAL + DW + APPLY persistence
✓ safe EOF progression
✓ binlog rotation support
✓ zero-event batch support
✓ idempotent retries
✓ committed LOAD replay safety
✓ latest CDC batch readiness validation
✓ safe fresh-install checkpoint bootstrap
✓ single-consumer Airflow orchestration
✓ fail-stop protection for pending staged work
✓ audit metrics and batch lineage
```

---

## 27. Current Limitations and Future Enhancements

The current implementation intentionally remains batch-oriented.

Remaining operational and production-oriented enhancements include:

* automatic recovery of pending staged batches when `READ > APPLY`;
* operational cleanup or retention policies for RAW and TRANSFORMED CDC staging;
* CDC lag and throughput metrics;
* alerting for checkpoint divergence;
* extended source-table coverage;
* additional failure-injection testing;
* production-oriented replication monitoring.

Pending staged work is currently protected by fail-stop semantics and recovered through controlled retry or task clearing rather than by automatically starting a new CDC batch.

These items represent future operational enhancements rather than unresolved correctness gaps in the current `v0.7.0` baseline.

---

## 28. Operational Documentation

Operational procedures are documented separately from this architecture design:

* [`local-reproduction.md`](local-reproduction.md) — clean local setup, Full Load bootstrap, CDC initialization, and platform startup.
* [`airflow-operations.md`](airflow-operations.md) — manual CDC execution, DagRun validation, cron scheduling, pause/unpause behavior, and schedule management.
