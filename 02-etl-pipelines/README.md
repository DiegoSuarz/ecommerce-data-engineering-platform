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
