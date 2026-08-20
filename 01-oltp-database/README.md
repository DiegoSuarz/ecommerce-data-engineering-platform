# OLTP Database

This module implements the operational data source for the **E-Commerce Data Engineering Platform**.

The original capstone used separate operational and analytical datasets. For this project, the source data was reorganized into a coherent OLTP model so that the complete platform can follow a true end-to-end data flow from MySQL to the analytical warehouse.

## Architecture

The OLTP database runs on **MySQL 8** inside Docker.

```text
Docker
└── MySQL
    └── sales
        ├── categories
        ├── countries
        └── orders
```

MySQL is exposed locally through port `3307` to avoid conflicts with an existing local MySQL installation using the default port `3306`.

## Data Model

### categories

Stores the available product categories.

| Column | Type | Description |
| --- | --- | --- |
| category_id | INT UNSIGNED | Primary key |
| category_name | VARCHAR(50) | Unique category name |

### countries

Stores the countries associated with sales orders.

| Column | Type | Description |
| --- | --- | --- |
| country_id | INT UNSIGNED | Primary key |
| country_name | VARCHAR(100) | Unique country name |

### orders

Stores the transactional sales records.

| Column | Type | Description |
| --- | --- | --- |
| order_id | BIGINT UNSIGNED | Primary key |
| order_date | DATE | Order date |
| country_id | INT UNSIGNED | Foreign key to countries |
| category_id | INT UNSIGNED | Foreign key to categories |
| amount | DECIMAL(12,2) | Order amount |

The table includes foreign-key constraints, a positive amount check, and indexes on the order date and foreign-key columns.

## Seed Data

The initial OLTP dataset contains:

| Dataset | Rows |
| --- | ---: |
| categories.csv | 5 |
| countries.csv | 56 |
| orders.csv | 300,000 |

The seed data was reconstructed from the analytical datasets of the original capstone so that the OLTP database and the future PostgreSQL Data Warehouse share a consistent data lineage.

The analytical `dateid` was converted back into the real order date before creating `orders.csv`. The OLTP therefore stores `order_date` rather than a Data Warehouse surrogate date key.

## Database Initialization

The OLTP database is initialized automatically by the official MySQL Docker entrypoint.

The initialization scripts are versioned in:

```text
sql/
├── 001_create_database.sql
├── 002_create_tables.sql
├── 003_load_initial_data.sql
└── 004_insert_incremental_test_data.sql
```

When the MySQL container starts with a new database volume, Docker automatically executes the first three initialization scripts in order:

```text
Docker Compose
      │
      ▼
MySQL Container
      │
      ├── Create database and application user
      │
      ├── 001_create_database.sql
      ├── 002_create_tables.sql
      └── 003_load_initial_data.sql
                  │
                  ▼
             Seed Dataset
```

The initialization process creates the `sales` database, creates the OLTP tables, provisions the application user, and loads the initial dataset.

The expected initial row counts are:

```text
categories = 5
countries  = 56
orders     = 300000
```

The initialization scripts are executed only when MySQL initializes a new database volume. Restarting an existing container does not reload the seed dataset.

### Incremental Test Data

`004_insert_incremental_test_data.sql` is intentionally excluded from the automatic Docker initialization process.

It contains additional records used to test incremental ETL behavior and should only be executed explicitly when required during development or testing.

## Application User

The MySQL application user is provisioned automatically by the official MySQL Docker entrypoint using the environment variables defined in `.env`.

The relevant configuration is:

```text
MYSQL_DATABASE
MYSQL_USER
MYSQL_PASSWORD
```

Database credentials are stored in the local `.env` file and are not committed to Git.

The application user is intended for application and ETL connectivity to the `sales` database.

## Running the Module

From the repository root, create the local environment file if it does not already exist:

```bash
cp .env.example .env
```

Configure the required credentials in `.env`, then start MySQL:

```bash
docker compose up -d mysql
```

On the first startup with a new database volume, Docker automatically:

1. Initializes MySQL.
2. Creates the `sales` database.
3. Creates the application user.
4. Creates the OLTP tables.
5. Loads the initial dataset.

No host-side Bash initialization scripts are required.

Check the container status:

```bash
docker compose ps
```

The MySQL service is exposed through the host port configured by `MYSQL_PORT`.

Because the initialization is handled inside Docker, the same setup can be used from Linux, WSL2, macOS, or Windows with Docker Desktop.

## Validation

The initialized OLTP database has been validated with:

```text
categories        5 rows
countries         56 rows
orders            300000 rows

order date range  2019-01-01 → 2021-12-31
amount range      9.00 → 8000.00

orphan countries  0
orphan categories 0
```

These values represent the initial seed dataset.
The orders table is expected to grow after initialization as new transactional and incremental test data is introduced.


## Data Warehouse Lineage

This OLTP model serves as the operational source for the PostgreSQL dimensional Data Warehouse:

```text
MySQL OLTP
│
├── categories ─────────► DimCategory
├── countries ──────────► DimCountry
└── orders
      ├── order_date ───► DimDate
      └─────────────────► FactSales

```

The Date Dimension is generated by the ETL process from the operational order_date.
The ETL supports both Full Load and incremental loading strategies.
