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


## Incremental Load

The ETL pipeline supports incremental loading using a simple watermark strategy based on `orders.order_id`.

The watermark represents the highest successfully processed order identifier.

```text
watermark = 300000
```

The incremental extraction reads only orders whose identifier is greater than the stored watermark:

```sql
SELECT
    order_id,
    order_date,
    country_id,
    category_id,
    amount
FROM orders
WHERE order_id > %s
ORDER BY order_id;
```

For example:

```text
Stored watermark
300000

New source rows
300001
300002
300003
300004
300005

Incremental extraction
300001 → 300005
```

After a successful load, the watermark advances to the highest processed `order_id`.

```text
300000
   ↓
process 300001–300005
   ↓
SUCCESS
   ↓
300005
```

The watermark is updated only after the incremental batch has been loaded and validated successfully.

If the pipeline fails before completion, the previous watermark remains unchanged so that the same batch can be safely processed again.

## Watermark Metadata

Incremental state is stored in:

```text
audit.pipeline_watermark
```

The table stores:

```text
pipeline_name
watermark_name
watermark_value
updated_at
```

For the current incremental pipeline:

```text
pipeline_name   = incremental_load
watermark_name  = orders_order_id
```

The watermark is initialized automatically when the incremental pipeline runs for the first time.

If no watermark exists, the pipeline reads:

```sql
MAX(order_id)
FROM dw.fact_sales
```

and uses the current Data Warehouse state as the initial watermark.

This allows the incremental pipeline to start immediately after a successful Full Load without manual watermark initialization.

## Incremental Staging

During Full Load, `staging.orders` contains the complete source dataset.

During Incremental Load, the same table acts as a temporary staging area for only the current incremental batch.

```text
Full Load
staging.orders
→ 300000 rows

Incremental Load
staging.orders
→ current batch only
```

For example:

```text
watermark = 300010

new orders:
300011
300012
300013
300014
300015

staging.orders:
300011 → 300015
```

The staging table is truncated before each incremental batch is loaded.

## Incremental Data Warehouse Load

The incremental pipeline does not rebuild the complete Data Warehouse.

Only new dimension values and fact rows are inserted.

For the Date Dimension:

```text
new order dates
      ↓
build date dimension rows
      ↓
INSERT
ON CONFLICT DO NOTHING
```

For the Sales Fact table:

```text
staging.orders
      ↓
dimension lookups
      ↓
dim_date
dim_country
dim_category
      ↓
fact_sales
```

Existing fact rows are protected using:

```sql
ON CONFLICT (order_id)
DO NOTHING;
```

This provides additional idempotency protection during retries.

## Incremental Reconciliation

Each incremental batch is reconciled between staging and the Data Warehouse.

The pipeline validates:

```text
staging row count
vs
loaded DW row count

staging amount total
vs
loaded DW amount total
```

The reconciliation compares only the orders included in the current staging batch.

## No-Op Execution

If no new source rows exist:

```text
watermark = 300015
MAX(source order_id) = 300015
```

the pipeline processes zero rows and completes successfully.

Example:

```text
rows_extracted = 0
rows_loaded    = 0
status         = SUCCESS
```

The watermark remains unchanged.

A no-op execution is considered a valid successful pipeline run, not an error.

## Failure and Retry Behavior

The incremental pipeline was tested with controlled failures.

Example:

```text
watermark = 300010

source batch:
300011 → 300015

staging load
      ↓
controlled failure
      ↓
status = FAILED
      ↓
watermark remains 300010
```

After removing the failure and executing the pipeline again:

```text
watermark = 300010
      ↓
same batch is extracted again
      ↓
300011 → 300015
      ↓
SUCCESS
      ↓
watermark = 300015
```

This behavior prevents data loss when an incremental execution fails.

## Automated Tests

The incremental pipeline includes automated tests with `pytest`.

The test suite validates:

- successful execution when no new data exists;
- watermark advancement after a successful incremental batch;
- watermark preservation when the pipeline fails.

The full ETL test suite currently contains seven tests:

```text
4 transformation tests
3 incremental pipeline tests
```

Run all tests with:

```bash
pytest -v 02-etl-pipelines/tests
```

## Running the Incremental Pipeline

After the infrastructure is initialized and a Full Load has been completed:

```bash
python 02-etl-pipelines/src/incremental_load.py
```

The pipeline automatically:

1. Starts an audit run.
2. Reads or initializes the watermark.
3. Extracts only new orders.
4. Loads the incremental staging batch.
5. Runs staging Data Quality checks.
6. Loads new Date Dimension records.
7. Resolves dimension lookups.
8. Loads new Sales Fact records.
9. Runs Data Warehouse quality checks.
10. Reconciles the incremental batch.
11. Updates the watermark only after success.
12. Completes the audit run.

## Simple Watermark Limitations

The current incremental strategy uses:

```text
orders.order_id
```

as a simple high-water mark.

This approach assumes that newly created orders receive monotonically increasing identifiers.

It supports:

```text
✓ consecutive IDs
✓ non-consecutive IDs
✓ gaps between IDs
```

Example:

```text
300016
300020
300035
```

All three rows are correctly extracted if the current watermark is `300015`.

However, the current strategy does not safely detect:

```text
✗ new rows inserted with an ID lower than the watermark
✗ updates to previously processed orders
✗ deleted source rows
✗ random or non-monotonic identifiers
```

A future version of the project will introduce a composite watermark based on a modification timestamp and `order_id`.

A later evolution will introduce Change Data Capture (CDC) to capture inserts, updates, and deletes more comprehensively.
