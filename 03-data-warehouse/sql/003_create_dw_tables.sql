-- ============================================================
-- E-Commerce Data Engineering Platform
-- PostgreSQL Data Warehouse Tables
-- ============================================================


-- ------------------------------------------------------------
-- Date Dimension
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dw.dim_date
(
    date_key      INTEGER PRIMARY KEY,
    full_date     DATE NOT NULL UNIQUE,
    year          SMALLINT NOT NULL,
    quarter       SMALLINT NOT NULL,
    quarter_name  VARCHAR(2) NOT NULL,
    month         SMALLINT NOT NULL,
    month_name    VARCHAR(10) NOT NULL,
    day           SMALLINT NOT NULL,
    weekday       SMALLINT NOT NULL,
    weekday_name  VARCHAR(10) NOT NULL,


CONSTRAINT chk_dim_date_quarter
    CHECK (quarter BETWEEN 1 AND 4),

CONSTRAINT chk_dim_date_month
    CHECK (month BETWEEN 1 AND 12),

CONSTRAINT chk_dim_date_day
    CHECK (day BETWEEN 1 AND 31),

CONSTRAINT chk_dim_date_weekday
    CHECK (weekday BETWEEN 1 AND 7)
);

-- ------------------------------------------------------------
-- Category Dimension
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dw.dim_category
(
    category_key  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category_id   INTEGER NOT NULL,
    category_name VARCHAR(50) NOT NULL
);


-- ------------------------------------------------------------
-- Country Dimension
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dw.dim_country
(
    country_key  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    country_id   INTEGER NOT NULL UNIQUE,
    country_name VARCHAR(100) NOT NULL
);


-- ------------------------------------------------------------
-- Sales Fact Table
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dw.fact_sales
(
    sales_key           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id            BIGINT NOT NULL UNIQUE,
    date_key            INTEGER NOT NULL,
    country_key         BIGINT NOT NULL,
    category_key        BIGINT NOT NULL,
    amount              NUMERIC(12,2) NOT NULL,
    source_updated_at   TIMESTAMPTZ NOT NULL,

    CONSTRAINT fk_fact_sales_date
        FOREIGN KEY (date_key)
        REFERENCES dw.dim_date (date_key),

    CONSTRAINT fk_fact_sales_country
        FOREIGN KEY (country_key)
        REFERENCES dw.dim_country (country_key),

    CONSTRAINT fk_fact_sales_category
        FOREIGN KEY (category_key)
        REFERENCES dw.dim_category (category_key),

    CONSTRAINT chk_fact_sales_amount
        CHECK (amount > 0),

    CONSTRAINT chk_fact_sales_order_id
        CHECK (order_id > 0)
);


CREATE INDEX IF NOT EXISTS ix_fact_sales_date_key
    ON dw.fact_sales (date_key);

CREATE INDEX IF NOT EXISTS ix_fact_sales_country_key
    ON dw.fact_sales (country_key);

CREATE INDEX IF NOT EXISTS ix_fact_sales_category_key
    ON dw.fact_sales (category_key);

CREATE INDEX IF NOT EXISTS ix_fact_sales_source_updated_at_order_id
ON dw.fact_sales (source_updated_at, order_id);
