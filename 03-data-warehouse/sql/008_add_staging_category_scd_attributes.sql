-- ============================================================
-- E-Commerce Data Engineering Platform
-- Staging Category SCD Source Attributes
-- ============================================================

ALTER TABLE staging.categories
    ADD COLUMN category_code VARCHAR(20),
    ADD COLUMN department VARCHAR(100),
    ADD COLUMN updated_at TIMESTAMPTZ;

ALTER TABLE staging.categories
    ALTER COLUMN category_code SET NOT NULL,
    ALTER COLUMN department SET NOT NULL,
    ALTER COLUMN updated_at SET NOT NULL;
