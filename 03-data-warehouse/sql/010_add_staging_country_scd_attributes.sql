ALTER TABLE staging.countries
    ADD COLUMN country_code VARCHAR(2),
    ADD COLUMN sales_region VARCHAR(50),
    ADD COLUMN market_segment VARCHAR(20),
    ADD COLUMN updated_at TIMESTAMPTZ,
    ADD COLUMN row_hash CHAR(64);
