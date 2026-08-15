-- ============================================================
-- E-Commerce Data Engineering Platform
-- OLTP Table Definitions
-- ============================================================

USE sales;

CREATE TABLE IF NOT EXISTS categories
(
    category_id   INT UNSIGNED NOT NULL,
    category_name VARCHAR(50) NOT NULL,

    CONSTRAINT pk_categories
        PRIMARY KEY (category_id),

    CONSTRAINT uq_categories_name
        UNIQUE (category_name)
);

CREATE TABLE IF NOT EXISTS countries
(
    country_id   INT UNSIGNED NOT NULL,
    country_name VARCHAR(100) NOT NULL,

    CONSTRAINT pk_countries
        PRIMARY KEY (country_id),

    CONSTRAINT uq_countries_name
        UNIQUE (country_name)
);

CREATE TABLE IF NOT EXISTS orders
(
    order_id    BIGINT UNSIGNED NOT NULL,
    order_date  DATE NOT NULL,
    country_id  INT UNSIGNED NOT NULL,
    category_id INT UNSIGNED NOT NULL,
    amount      DECIMAL(12,2) NOT NULL,

    CONSTRAINT pk_orders
        PRIMARY KEY (order_id),

    CONSTRAINT fk_orders_country
        FOREIGN KEY (country_id)
        REFERENCES countries (country_id),

    CONSTRAINT fk_orders_category
        FOREIGN KEY (category_id)
        REFERENCES categories (category_id),

    CONSTRAINT chk_orders_amount
        CHECK (amount > 0),

    INDEX ix_orders_order_date (order_date),
    INDEX ix_orders_country_id (country_id),
    INDEX ix_orders_category_id (category_id)
);
