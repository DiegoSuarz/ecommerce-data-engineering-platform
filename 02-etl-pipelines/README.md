# ETL Pipelines

This module contains the Full Load and Change Data Capture (CDC) pipelines used to bootstrap and continuously synchronize e-commerce sales data from the MySQL OLTP source into PostgreSQL analytical and CDC structures.

## Architecture

The module has two final runtime responsibilities:

```text
Full Load
    MySQL current state
          ↓
    PostgreSQL staging
          ↓
    Data Warehouse rebuild

CDC
    MySQL binary log
          ↓
    RAW
          ↓
    TRANSFORMED
          ↓
    LOAD / APPLY
          ├── FINAL CDC event
          ├── DW mutation
          └── APPLY checkpoint
```

Full Load is the bootstrap/reconstruction path.

CDC is the continuous synchronization path.

## ETL Flow

Full Load rebuilds the warehouse from the current source state.

CDC consumes committed row-level changes from the MySQL binary log,
persists them durably through RAW and TRANSFORMED stages, and applies
each normalized event to both the FINAL CDC store and the analytical
warehouse.

Event ordering is preserved throughout CDC processing, and checkpoint
advancement is coupled transactionally with durable downstream effects.

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



### `cdc/stream.py`

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

### `cdc/extract.py`

Implements the CDC EXTRACT stage.

It reads committed transactions from the MySQL binary log and persists them into:

```text
cdc.raw_change_event
```

RAW event persistence and advancement of the READ checkpoint occur in the same PostgreSQL transaction.

### `cdc/transform.py`

Implements the CDC TRANSFORM stage.

It reads RAW events, validates operation semantics, extracts source primary keys, and persists normalized events into:

```text
cdc.transformed_event
```

The transformation stage maintains lineage between each RAW event and its transformed representation.

### `cdc/apply.py`

Implements the CDC LOAD/APPLY stage.

It validates that a batch is ready for loading, reads transformed events, and persists final events into:

```text
cdc.change_event
```

Final event persistence, analytical Data Warehouse mutation, and advancement of the APPLY checkpoint occur atomically in the same PostgreSQL transaction.

### `cdc/pipeline.py`

Coordinates the multi-stage CDC execution.

Its responsibilities include:

* verifying CDC checkpoint readiness;
* bootstrapping APPLY from the current MySQL binlog head only when CDC state is fresh;
* initializing the READ checkpoint from APPLY when required;
* refusing automatic bootstrap when durable CDC history already exists;
* starting CDC audit runs and batches;
* completing CDC audit metrics;
* finalizing successful ETL runs;
* marking failed runs when downstream processing fails.

A new CDC batch can begin only when the READ and APPLY checkpoints are aligned.

## Full Load Strategy

The current pipeline implements a Full Load strategy.

Before loading data, the target tables are truncated and rebuilt from the source data.

This makes the Full Load idempotent: running the pipeline multiple times with the same source data produces the same final Data Warehouse state.

The Full Load remains available as the complete rebuild strategy. After bootstrap, log-based CDC owns continuous synchronization of the analytical warehouse.

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
135 passed
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

On a completely fresh CDC installation, where both checkpoints and CDC batch
history are absent, the pipeline initializes APPLY from the current MySQL
binary-log head and then initializes READ from APPLY.

Automatic APPLY bootstrap is fail-closed. If APPLY is missing while READ or
CDC batch history already exists, the pipeline raises an error instead of
moving the checkpoint forward and potentially skipping durable work.

Checkpoint bootstrap is idempotent. Once APPLY exists, later readiness checks
reuse the durable coordinate rather than resetting it to the current MySQL
binary-log head.

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
