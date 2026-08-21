-- ============================================================
-- E-Commerce Data Engineering Platform
-- PostgreSQL Staging Tables
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.categories
(
    category_id   INTEGER NOT NULL,
    category_name VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS staging.countries
(
    country_id   INTEGER NOT NULL,
    country_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS staging.orders
(
    order_id    BIGINT NOT NULL,
    order_date  DATE NOT NULL,
    country_id  INTEGER NOT NULL,
    category_id INTEGER NOT NULL,
    amount      NUMERIC(12,2) NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL
);
