-- ============================================================
-- E-Commerce Data Engineering Platform
-- Category Dimension SCD Attributes
-- ============================================================

ALTER TABLE dw.dim_category
    ADD COLUMN category_code VARCHAR(20),
    ADD COLUMN department VARCHAR(100),
    ADD COLUMN effective_from TIMESTAMPTZ,
    ADD COLUMN effective_to TIMESTAMPTZ,
    ADD COLUMN is_current BOOLEAN;


UPDATE dw.dim_category
SET
    category_code = CASE category_id
        WHEN 1 THEN 'ELEC'
        WHEN 2 THEN 'BOOK'
        WHEN 3 THEN 'TOYS'
        WHEN 4 THEN 'SPRT'
        WHEN 5 THEN 'SOFT'
    END,
    department = CASE category_id
        WHEN 1 THEN 'General Merchandise'
        WHEN 2 THEN 'Media'
        WHEN 3 THEN 'General Merchandise'
        WHEN 4 THEN 'Sporting Goods'
        WHEN 5 THEN 'Technology'
    END,
    effective_from = TIMESTAMPTZ '2019-01-01 00:00:00+00',
    effective_to = NULL,
    is_current = TRUE
WHERE category_id IN (1, 2, 3, 4, 5);

ALTER TABLE dw.dim_category
    ALTER COLUMN category_code SET NOT NULL,
    ALTER COLUMN department SET NOT NULL,
    ALTER COLUMN effective_from SET NOT NULL,
    ALTER COLUMN is_current SET NOT NULL;


ALTER TABLE dw.dim_category
    ADD CONSTRAINT chk_dim_category_effective_period
    CHECK (
        effective_to IS NULL
        OR effective_to > effective_from
    );

ALTER TABLE dw.dim_category
    ADD CONSTRAINT chk_dim_category_current_period
    CHECK (
        (is_current = TRUE AND effective_to IS NULL)
        OR
        (is_current = FALSE AND effective_to IS NOT NULL)
    );

CREATE UNIQUE INDEX uq_dim_category_current_category_id
ON dw.dim_category (category_id)
WHERE is_current = TRUE;

CREATE INDEX ix_dim_category_category_id_effective_period
ON dw.dim_category
(
    category_id,
    effective_from,
    effective_to
);
