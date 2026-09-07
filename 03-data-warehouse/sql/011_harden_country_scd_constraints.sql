-- Harden staging.countries after SCD attributes have been backfilled
-- and the ETL has been updated to always provide valid values.

ALTER TABLE staging.countries
    ALTER COLUMN country_code SET NOT NULL,
    ALTER COLUMN sales_region SET NOT NULL,
    ALTER COLUMN market_segment SET NOT NULL,
    ALTER COLUMN updated_at SET NOT NULL,
    ALTER COLUMN row_hash SET NOT NULL;

ALTER TABLE staging.countries
    ADD CONSTRAINT chk_staging_countries_row_hash_format
    CHECK (row_hash ~ '^[0-9a-f]{64}$');


-- Harden dw.dim_country after existing rows have received their
-- SCD2 row hashes.

ALTER TABLE dw.dim_country
    ALTER COLUMN row_hash SET NOT NULL;

ALTER TABLE dw.dim_country
    ADD CONSTRAINT chk_dim_country_row_hash_format
    CHECK (row_hash ~ '^[0-9a-f]{64}$');
