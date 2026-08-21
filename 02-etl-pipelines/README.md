# ETL Pipelines

This module contains the ETL pipeline used to move e-commerce sales data from the MySQL OLTP source database into the PostgreSQL Data Warehouse.

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

## Full Load Strategy

The current pipeline implements a Full Load strategy.

Before loading data, the target tables are truncated and rebuilt from the source data.

This makes the Full Load idempotent: running the pipeline multiple times with the same source data produces the same final Data Warehouse state.

Incremental loading is outside the scope of this stage and can be added in a later module.

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

Unit tests are implemented with `pytest`.

Run all ETL tests from the repository root:

```bash
pytest -v 02-etl-pipelines/tests
```

The current tests validate Date Dimension transformation logic, including:

- Date attribute generation.
- Duplicate removal.
- Quarter boundaries.
- Date ordering.

A successful test execution currently returns:

```text
4 passed
```

## Dependencies

Python dependencies are defined in the root `requirements.txt`.

Install them with:

```bash
pip install -r requirements.txt
```

The ETL currently uses:

- `mysql-connector-python`
- `psycopg`
- `pytest`

## Current Scope

This module currently implements:

- MySQL source extraction.
- Batch extraction for large order datasets.
- PostgreSQL staging.
- Full Load strategy.
- Date Dimension transformation.
- Dimension loading.
- Surrogate keys.
- Dimension lookups.
- Fact table loading.
- Data Quality checks.
- Staging-to-DW reconciliation.
- ETL auditing.
- Structured logging.
- Failure handling.
- Idempotent execution.
- Unit testing.

Incremental loading and external workflow orchestration are not part of the current Full Load implementation and will be introduced in later stages of the project.


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
pytest -v -m "not integration" 02-etl-pipelines/tests
```

Run only integration tests:

```bash
pytest -v -m integration 02-etl-pipelines/tests
```

Run the complete test suite:

```bash
pytest -v 02-etl-pipelines/tests
```

Current expected result:

```text
11 passed
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

## Current Limitation

The composite watermark detects:

```text
✓ new INSERTs
✓ UPDATEs to existing rows
✓ non-consecutive order IDs
✓ rows sharing the same timestamp
```

It does not detect physical DELETE operations from the source.

A later evolution of the project can introduce Change Data Capture (CDC) for insert, update, and delete event capture.
