# ETL Pipelines

This module contains the full-load, incremental, and Change Data Capture (CDC) pipelines used to move and synchronize e-commerce sales data from the MySQL OLTP source database into PostgreSQL analytical and CDC structures.

## Architecture

The Full Load follows this data flow:

```text
MySQL OLTP
    |
    v
Extract
    |
    v
PostgreSQL Staging
    |
    v
Data Quality Checks
    |
    v
Transform
    |
    v
Dimensions
    |
    v
Fact Table
    |
    v
DW Quality Checks
    |
    v
Reconciliation
```

Pipeline executions are also registered in the audit layer.

## ETL Flow

The Full Load performs the following steps:

1. Start an ETL audit run.
2. Extract categories, countries, and orders from MySQL.
3. Load the extracted data into PostgreSQL staging tables.
4. Run staging Data Quality checks.
5. Build the Date Dimension.
6. Load the Date, Category, and Country dimensions.
7. Resolve surrogate keys through dimension lookups.
8. Load the Sales Fact table.
9. Run Data Warehouse quality checks.
10. Reconcile staging data with the Data Warehouse.
11. Complete the audit run as `SUCCESS`.

If an error occurs, the pipeline logs the exception and marks the audit run as `FAILED`.

## Source Modules

### `db.py`

Creates database connections for:

- MySQL OLTP source database.
- PostgreSQL Data Warehouse.

Connection settings are loaded from environment variables.

### `extract.py`

Extracts source data from MySQL.

The orders table is extracted in batches to avoid loading the complete dataset into memory at once.

### `load.py`

Loads data into:

- PostgreSQL staging tables.
- Data Warehouse dimensions.
- Sales Fact table.

The Full Load uses a truncate-and-reload strategy to support safe re-execution.

### `transform.py`

Contains transformation logic used by the pipeline.

It currently builds the Date Dimension attributes from distinct order dates.

### `quality.py`

Contains Data Quality checks for staging and the Data Warehouse.

The checks validate:

- Duplicate orders.
- NULL values.
- Invalid amounts.
- Orphan country references.
- Orphan category references.
- Invalid dimension keys.

It also reconciles row counts and sales amounts between staging and the Data Warehouse.

### `audit.py`

Registers pipeline executions in `audit.etl_run`.

Each execution stores:

- Pipeline name.
- Start and finish timestamps.
- Status.
- Extracted rows.
- Loaded rows.
- Rejected rows.
- Error message.

### `logger.py`

Provides the common logging configuration used by the ETL pipeline.

Logs include:

- Timestamp.
- Log level.
- Module name.
- Message.

### `full_load.py`

Orchestrates the complete Full Load pipeline.

It coordinates extraction, staging loads, transformations, Data Quality checks, Data Warehouse loads, reconciliation, logging, and auditing.

### `incremental_load.py`

Orchestrates the incremental ETL pipeline.

It uses the composite `(updated_at, order_id)` watermark to detect new and updated source rows, processes dimension changes independently, performs quality and reconciliation checks, and advances the watermark only after successful completion.

### `cdc.py`

Contains the core MySQL binlog CDC primitives.

Its responsibilities include:

* creating the MySQL binlog stream;
* normalizing row events;
* identifying INSERT, UPDATE, and DELETE operations;
* buffering events until transaction commit;
* generating deterministic event keys;
* extracting primary keys;
* comparing binlog coordinates;
* supporting binlog rotation.

Only committed source transactions are exposed for durable CDC processing.

### `cdc_extract.py`

Implements the CDC EXTRACT stage.

It reads committed transactions from the MySQL binary log and persists them into:

```text
cdc.raw_change_event
```

RAW event persistence and advancement of the READ checkpoint occur in the same PostgreSQL transaction.

### `cdc_transform.py`

Implements the CDC TRANSFORM stage.

It reads RAW events, validates operation semantics, extracts source primary keys, and persists normalized events into:

```text
cdc.transformed_event
```

The transformation stage maintains lineage between each RAW event and its transformed representation.

### `cdc_load.py`

Implements the CDC LOAD stage.

It validates that a batch is ready for loading, reads transformed events, and persists final events into:

```text
cdc.change_event
```

Final event persistence and advancement of the APPLY checkpoint occur atomically.

### `cdc_pipeline.py`

Coordinates the multi-stage CDC execution.

Its responsibilities include:

* verifying CDC checkpoint readiness;
* initializing the READ checkpoint from APPLY when required;
* starting CDC audit runs and batches;
* completing CDC audit metrics;
* finalizing successful ETL runs;
* marking failed runs when downstream processing fails.

A new CDC batch can begin only when the READ and APPLY checkpoints are aligned.

## Full Load Strategy

The current pipeline implements a Full Load strategy.

Before loading data, the target tables are truncated and rebuilt from the source data.

This makes the Full Load idempotent: running the pipeline multiple times with the same source data produces the same final Data Warehouse state.

The Full Load remains available as the complete rebuild strategy. Incremental loading and log-based CDC are implemented as separate processing modes and are documented later in this module.

## Running the Pipeline

From the repository root, activate the Python virtual environment:

```bash
source .venv/bin/activate
```

Load the environment variables:

```bash
set -a
source .env
set +a
```

Make sure the database services are running:

```bash
docker compose ps
```

Both database services should be available:

```text
ecommerce-mysql
ecommerce-postgres
```

Run the Full Load:

```bash
python 02-etl-pipelines/src/full_load.py
```

## Expected Result

A successful Full Load processes:

- 5 categories.
- 56 countries.
- 300,000 orders.
- 1,096 Date Dimension rows.
- 300,000 Sales Fact rows.

The pipeline currently records the following execution metrics:

```text
rows_extracted = 300061
rows_loaded    = 301157
rows_rejected  = 0
status         = SUCCESS
```

The audit information is stored in:

```text
audit.etl_run
```

## Idempotency

The Full Load is designed to be safely re-executed.

Staging and Data Warehouse target tables are truncated before being rebuilt.

Therefore, executing the pipeline multiple times with unchanged source data does not create duplicate records.

For example:

```text
First execution
dw.fact_sales = 300000 rows

Second execution
dw.fact_sales = 300000 rows
```

## Data Quality

Data Quality checks are executed before and after the Data Warehouse load.

### Staging Checks

The staging layer validates:

- Duplicate orders.
- NULL values.
- Invalid sales amounts.
- Orphan country references.
- Orphan category references.

### Data Warehouse Checks

The Data Warehouse validates dimension references for:

- Date keys.
- Country keys.
- Category keys.

A valid execution should return zero invalid references.

## Reconciliation

After loading the Data Warehouse, the pipeline compares staging and fact data.

The reconciliation currently validates:

- Row count.
- Total sales amount.

Expected values:

```text
staging.orders rows = 300000
dw.fact_sales rows  = 300000

staging total amount = 1201006258.00
DW total amount      = 1201006258.00
```

Matching values confirm that the complete sales dataset reached the Data Warehouse.

## Audit and Failure Handling

Every Full Load execution creates a record in:

```text
audit.etl_run
```

A successful execution is stored as:

```text
SUCCESS
```

If an exception occurs during the pipeline, the execution is stored as:

```text
FAILED
```

The corresponding error message is also persisted.

This provides execution history and makes pipeline failures traceable.

## Logging

The pipeline uses structured logging instead of relying only on `print()` statements.

The log format contains:

```text
timestamp | level | module | message
```

Example:

```text
2026-08-18 17:23:54 | INFO | full_load | Starting full load
2026-08-18 17:24:10 | INFO | load | Orders batch 30 loaded: rows=10000 total=300000
2026-08-18 17:24:24 | INFO | full_load | Full load completed successfully
```

Orders are loaded in batches of 10,000 rows, and each batch is logged during execution.

Errors are logged with their traceback to simplify debugging.

## Tests

Automated tests are implemented with `pytest`.

Run all ETL and CDC tests from the repository root:

```bash
PYTHONPATH=02-etl-pipelines/src \
pytest -q 02-etl-pipelines/tests
```

The test suite covers:

* transformation logic;
* staging and Data Warehouse loading;
* Data Quality checks;
* ETL audit behavior;
* reconciliation;
* composite watermark ordering and advancement;
* incremental update detection;
* Slowly Changing Dimension behavior;
* MySQL CDC row-event normalization;
* committed CDC transaction handling;
* binlog coordinate comparison and rotation;
* durable RAW extraction;
* CDC transformation semantics and primary-key extraction;
* atomic final CDC loading;
* READ/APPLY checkpoint behavior;
* CDC pipeline coordination and audit finalization;
* retry-safe and idempotent persistence.

The validated M8 test result is:

```text
125 passed
```

## Dependencies

Python dependencies are defined in the root `requirements.txt`.

Install them with:

```bash
pip install -r requirements.txt
```

The pipeline uses:

* `mysql-connector-python` for standard MySQL ETL access;
* `psycopg` for PostgreSQL access;
* `mysql-replication` for MySQL binary-log CDC;
* `pytest` for automated testing.

The Airflow runtime contains its own CDC-specific dependency file under:

```text
airflow/requirements.txt
```

## Current Scope

This module currently implements:

* MySQL source extraction.
* Batch extraction for large order datasets.
* PostgreSQL staging.
* Full Load processing.
* Composite-watermark incremental processing.
* Incremental INSERT and UPDATE detection.
* Date Dimension transformation.
* Slowly Changing Dimensions.
* Historical dimension versioning.
* Temporal surrogate-key resolution.
* Dimension and fact loading.
* Data Quality checks.
* Staging-to-DW reconciliation.
* ETL auditing.
* Structured logging.
* Failure handling.
* Retry-safe and idempotent execution.
* Log-based MySQL Change Data Capture.
* INSERT, UPDATE, and DELETE event capture.
* Transaction-aware CDC processing.
* Durable RAW and TRANSFORMED CDC staging.
* Final CDC event persistence.
* READ/APPLY CDC checkpoint management.
* Safe EOF checkpoint progression.
* Binlog rotation handling.
* Automated unit and integration testing.

Apache Airflow orchestration is implemented separately under the repository's `airflow` module.

## Composite Watermark Incremental Load

The incremental ETL pipeline now uses a composite watermark based on:

```text
(updated_at, order_id)
```

The timestamp identifies when a source row was last modified, while `order_id` acts as a deterministic tie-breaker when multiple rows share the same timestamp.

The current watermark is stored in:

```text
audit.pipeline_watermark
```

using:

```text
watermark_timestamp
watermark_order_id
```

For example:

```text
watermark_timestamp = 2026-08-21 03:00:00+00
watermark_order_id  = 300002
```

### Incremental Extraction

The source query processes rows that come after the current composite watermark:

```sql
SELECT
    order_id,
    order_date,
    country_id,
    category_id,
    amount,
    updated_at
FROM orders
WHERE
    updated_at > %s
    OR (
        updated_at = %s
        AND order_id > %s
    )
ORDER BY
    updated_at,
    order_id;
```

This provides lexicographic ordering:

```text
(updated_at, order_id)
```

For example:

```text
Current watermark:
(2026-08-21 03:00:00, 300001)

Source rows:
(2026-08-21 03:00:00, 300001)
(2026-08-21 03:00:00, 300002)

Extracted:
(2026-08-21 03:00:00, 300002)
```

The timestamp is compared first. If two rows have the same timestamp, `order_id` determines the next row.

## Source Modification Tracking

The MySQL `orders` table includes:

```sql
updated_at TIMESTAMP(6) NOT NULL
    DEFAULT CURRENT_TIMESTAMP(6)
    ON UPDATE CURRENT_TIMESTAMP(6)
```

The value is generated when the row is created and automatically refreshed when the source row is updated.

The timestamp is propagated through the pipeline:

```text
MySQL orders.updated_at
        ↓
staging.orders.updated_at
        ↓
dw.fact_sales.source_updated_at
```

This allows the Data Warehouse to preserve the source modification timestamp.

## Automatic Watermark Bootstrap

If no composite watermark exists, the pipeline initializes it from the latest successfully loaded Data Warehouse row:

```sql
SELECT
    source_updated_at,
    order_id
FROM dw.fact_sales
ORDER BY
    source_updated_at DESC,
    order_id DESC
LIMIT 1;
```

This allows the incremental pipeline to start immediately after a successful Full Load.

If the Data Warehouse is empty, the pipeline uses a baseline watermark equivalent to:

```text
(datetime.min UTC, 0)
```

## Update Detection

The previous simple watermark based only on `order_id` could detect new rows whose identifiers were greater than the stored watermark.

However, it could not detect updates to previously processed orders.

Example:

```text
Previous watermark:
order_id = 300000

Updated source row:
order_id = 150000
```

A simple watermark would evaluate:

```text
150000 > 300000
→ false
```

The composite watermark instead evaluates the modification timestamp:

```text
previous:
(2026-08-21 02:39:01, 300000)

updated row:
(2026-08-21 02:51:42, 150000)

result:
updated timestamp is newer
→ row is extracted
```

## Incremental Fact Upsert

Because existing orders can now be re-extracted after an update, the incremental Data Warehouse load uses an UPSERT strategy.

```sql
ON CONFLICT (order_id)
DO UPDATE
SET
    date_key = EXCLUDED.date_key,
    country_key = EXCLUDED.country_key,
    category_key = EXCLUDED.category_key,
    amount = EXCLUDED.amount,
    source_updated_at = EXCLUDED.source_updated_at;
```

This means the pipeline supports:

```text
new source order
→ INSERT into fact_sales

updated existing order
→ UPDATE existing fact_sales row
```

## Watermark Advancement

The composite watermark advances only after:

```text
Extract
   ↓
Staging Load
   ↓
Staging Quality Checks
   ↓
Dimension / Fact Load
   ↓
DW Quality Checks
   ↓
Reconciliation
   ↓
SUCCESS
   ↓
Update Composite Watermark
```

If the pipeline fails before completion, the previous watermark remains unchanged.

This preserves safe retry behavior.

## Composite Watermark Tests

The test suite includes both unit and integration tests.

Unit tests validate:

- successful no-op execution;
- watermark advancement after success;
- watermark preservation after failure;
- tuple ordering by timestamp and `order_id`;
- tie-breaking when timestamps are equal.

Integration tests validate real MySQL/PostgreSQL behavior:

- extraction when two rows share the same `updated_at`;
- detection of an update to an old `order_id` using a newer timestamp.

Run only unit tests:

```bash
PYTHONPATH=02-etl-pipelines/src \
pytest -v -m "not integration" 02-etl-pipelines/tests
```

Run only integration tests:

```bash
PYTHONPATH=02-etl-pipelines/src \
pytest -v -m integration 02-etl-pipelines/tests
```

Run the complete test suite:

```bash
PYTHONPATH=02-etl-pipelines/src \
pytest -v 02-etl-pipelines/tests
```

```text
The repository-wide test suite now includes Full Load, incremental, SCD, and CDC coverage.

For the current overall validated result, see the main `Tests` section above.
```

## Evolution from Simple to Composite Watermark

The project intentionally evolved through two incremental strategies:

```text
v0.2.0
Simple Watermark
(order_id)

        ↓

v0.3.0
Composite Watermark
(updated_at, order_id)
```

The simple watermark remains useful for understanding the fundamentals of stateful incremental loading.

The composite watermark adds support for updates to previously processed rows and deterministic ordering when multiple source records share the same modification timestamp.

## Evolution from Composite Watermark to CDC

The composite watermark pipeline provides state-based incremental processing.

It detects:

```text
✓ new INSERTs
✓ UPDATEs to existing rows
✓ non-consecutive order IDs
✓ rows sharing the same timestamp
```

However, a source query cannot detect a row after that row has been physically deleted.

The project therefore evolves from state-based incremental extraction to log-based Change Data Capture:

```text
Composite Watermark
(updated_at, order_id)
        │
        │  state-based
        ▼
INSERT + UPDATE detection

        ↓

MySQL Binary Log CDC
        │
        │  event-based
        ▼
INSERT + UPDATE + DELETE capture
```

The two strategies serve different purposes and coexist in the platform.

The composite watermark pipeline maintains the analytical Data Warehouse incrementally, while CDC preserves the source change stream.

## Change Data Capture

The CDC implementation reads MySQL row events directly from the binary log.

The current source scope includes:

```text
sales.categories
sales.countries
sales.orders
```

Only committed MySQL transactions are propagated to durable CDC storage.

### CDC Architecture

CDC processing is separated into independent stages:

```text
MySQL Binary Log
        │
        ▼
     EXTRACT
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
        ▼
cdc.change_event
```

The intermediate CDC layers are durable PostgreSQL tables rather than Airflow XCom payloads.

### Transaction and Event Model

Row events are buffered until their MySQL transaction commit boundary is observed.

Each captured row change receives a deterministic event key based on:

```text
binlog_file:event_end_position:row_index
```

The event model preserves:

* operation type;
* source schema and table;
* primary key;
* before and after row images;
* binlog coordinates;
* transaction identifier;
* commit position;
* event and commit timestamps.

### Durable CDC Staging

EXTRACT persists committed source events into:

```text
cdc.raw_change_event
```

TRANSFORM validates event semantics, extracts primary keys, and writes:

```text
cdc.transformed_event
```

LOAD writes the final normalized events into:

```text
cdc.change_event
```

This produces explicit lineage:

```text
RAW
 ↓
TRANSFORMED
 ↓
FINAL
```

### READ and APPLY Checkpoints

CDC uses two checkpoints:

```text
READ
mysql_sales_binlog_read

APPLY
mysql_sales_binlog
```

READ represents the source position that has been durably captured into RAW staging.

APPLY represents the source position that has been durably applied to the final CDC event store.

A new batch is allowed to begin only when:

```text
READ == APPLY
```

If READ is ahead of APPLY, the pipeline fails fast because durable staged work still needs to be completed.

### Atomicity and Idempotency

The CDC pipeline provides two important atomic boundaries:

```text
RAW persistence
+
READ checkpoint
```

and:

```text
FINAL persistence
+
APPLY checkpoint
```

Each pair is committed in a single PostgreSQL transaction.

CDC processing is retry-safe through deterministic event keys and unique constraints.

Reprocessing the same source event does not create duplicate final CDC records.

### Safe EOF and Binlog Rotation

The reader can safely advance to the current stream position after normal non-blocking stream exhaustion, even when no relevant transaction was captured.

An exception does not advance this safe EOF coordinate.

Checkpoint comparison also supports MySQL binlog rotation, for example:

```text
binlog.000032:3771
        ↓
binlog.000033:1120
```

### CDC Validation

The multi-stage CDC pipeline was validated with one committed source transaction containing:

```text
1 INSERT
1 UPDATE
1 DELETE
```

The resulting batch produced:

```text
RAW          = 3
TRANSFORMED  = 3
FINAL        = 3

transactions = 1
events       = 3
INSERT       = 1
UPDATE       = 1
DELETE       = 1
```

The execution also crossed a real MySQL binlog rotation and completed with aligned READ and APPLY checkpoints.

A subsequent no-op batch completed successfully with zero transactions and zero events while preserving checkpoint equality.

For the full CDC architecture, checkpoint semantics, failure model, and validation evidence, see:

[`../docs/cdc-design.md`](../docs/cdc-design.md)
