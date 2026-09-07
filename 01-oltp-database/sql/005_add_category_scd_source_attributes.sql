-- ============================================================
-- E-Commerce Data Engineering Platform
-- Category SCD Source Attributes
-- ============================================================

USE sales;

ALTER TABLE categories
    ADD COLUMN category_code VARCHAR(20) NULL,
    ADD COLUMN department VARCHAR(100) NULL,
    ADD COLUMN updated_at TIMESTAMP(6) NULL;

UPDATE categories
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
    updated_at = CURRENT_TIMESTAMP(6)
WHERE category_id IN (1, 2, 3, 4, 5);


ALTER TABLE categories
    MODIFY COLUMN category_code VARCHAR(20) NOT NULL,
    MODIFY COLUMN department VARCHAR(100) NOT NULL,
    MODIFY COLUMN updated_at TIMESTAMP(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),

    ADD CONSTRAINT uq_categories_code
        UNIQUE (category_code),

    ADD INDEX ix_categories_updated_at_category_id
        (updated_at, category_id);
