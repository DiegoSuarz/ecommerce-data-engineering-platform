-- ============================================================
-- E-Commerce Data Engineering Platform
-- Initial OLTP Data Load
-- ============================================================

USE sales;

LOAD DATA LOCAL INFILE '/docker-entrypoint-data/categories.csv'
INTO TABLE categories
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    category_id,
    category_name
);

LOAD DATA LOCAL INFILE '/docker-entrypoint-data/countries.csv'
INTO TABLE countries
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    country_id,
    country_name
);

LOAD DATA LOCAL INFILE '/docker-entrypoint-data/orders.csv'
INTO TABLE orders
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    order_id,
    order_date,
    country_id,
    category_id,
    amount
);
