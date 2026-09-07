ALTER TABLE dw.dim_country
    ADD COLUMN country_code VARCHAR(2),
    ADD COLUMN sales_region VARCHAR(50),
    ADD COLUMN market_segment VARCHAR(20),
    ADD COLUMN row_hash CHAR(64),
    ADD COLUMN effective_from TIMESTAMPTZ,
    ADD COLUMN effective_to TIMESTAMPTZ,
    ADD COLUMN is_current BOOLEAN;

UPDATE dw.dim_country
SET
    country_code = CASE country_id
        WHEN 1 THEN 'AR'
        WHEN 2 THEN 'AU'
        WHEN 3 THEN 'AT'
        WHEN 4 THEN 'AZ'
        WHEN 5 THEN 'BE'
        WHEN 6 THEN 'BR'
        WHEN 7 THEN 'BG'
        WHEN 8 THEN 'CA'
        WHEN 9 THEN 'CY'
        WHEN 10 THEN 'CZ'
        WHEN 11 THEN 'DK'
        WHEN 12 THEN 'EG'
        WHEN 13 THEN 'EE'
        WHEN 14 THEN 'FI'
        WHEN 15 THEN 'FR'
        WHEN 16 THEN 'DE'
        WHEN 17 THEN 'GR'
        WHEN 18 THEN 'HU'
        WHEN 19 THEN 'IN'
        WHEN 20 THEN 'ID'
        WHEN 21 THEN 'IE'
        WHEN 22 THEN 'IL'
        WHEN 23 THEN 'IT'
        WHEN 24 THEN 'JP'
        WHEN 25 THEN 'JO'
        WHEN 26 THEN 'MY'
        WHEN 27 THEN 'MX'
        WHEN 28 THEN 'NL'
        WHEN 29 THEN 'NZ'
        WHEN 30 THEN 'NO'
        WHEN 31 THEN 'OM'
        WHEN 32 THEN 'PE'
        WHEN 33 THEN 'PH'
        WHEN 34 THEN 'PL'
        WHEN 35 THEN 'PT'
        WHEN 36 THEN 'QA'
        WHEN 37 THEN 'RU'
        WHEN 38 THEN 'SA'
        WHEN 39 THEN 'SG'
        WHEN 40 THEN 'ZA'
        WHEN 41 THEN 'KR'
        WHEN 42 THEN 'ES'
        WHEN 43 THEN 'SD'
        WHEN 44 THEN 'SE'
        WHEN 45 THEN 'CH'
        WHEN 46 THEN 'TW'
        WHEN 47 THEN 'TJ'
        WHEN 48 THEN 'TH'
        WHEN 49 THEN 'TR'
        WHEN 50 THEN 'UA'
        WHEN 51 THEN 'AE'
        WHEN 52 THEN 'GB'
        WHEN 53 THEN 'US'
        WHEN 54 THEN 'UY'
        WHEN 55 THEN 'UZ'
        WHEN 56 THEN 'VN'
    END,

    sales_region = CASE
        WHEN country_id IN (1, 6, 27, 32, 54)
            THEN 'LATAM'

        WHEN country_id IN (8, 53)
            THEN 'NORTH_AMERICA'

        WHEN country_id IN (
            3, 5, 7, 9, 10, 11, 13, 14, 15, 16,
            17, 18, 21, 23, 28, 30, 34, 35, 42, 44,
            45, 50, 52
        )
            THEN 'EUROPE'

        WHEN country_id IN (
            22, 25, 31, 36, 38, 49, 51
        )
            THEN 'MIDDLE_EAST'

        WHEN country_id IN (12, 40, 43)
            THEN 'AFRICA'

        WHEN country_id IN (
            2, 19, 20, 24, 26, 29, 33, 39,
            41, 46, 48, 56
        )
            THEN 'ASIA_PACIFIC'

        WHEN country_id IN (4, 37, 47, 55)
            THEN 'CENTRAL_ASIA'
    END,

    market_segment = CASE country_id % 3
        WHEN 0 THEN 'STANDARD'
        WHEN 1 THEN 'GROWTH'
        WHEN 2 THEN 'STRATEGIC'
    END,

    effective_from = TIMESTAMPTZ '2019-01-01 00:00:00+00',
    effective_to = NULL,
    is_current = TRUE
WHERE country_id BETWEEN 1 AND 56;

ALTER TABLE dw.dim_country
    ALTER COLUMN country_code SET NOT NULL,
    ALTER COLUMN sales_region SET NOT NULL,
    ALTER COLUMN market_segment SET NOT NULL,
    ALTER COLUMN effective_from SET NOT NULL,
    ALTER COLUMN is_current SET NOT NULL;

ALTER TABLE dw.dim_country
    ADD CONSTRAINT chk_dim_country_effective_period
    CHECK (
        effective_to IS NULL
        OR effective_to > effective_from
    );

ALTER TABLE dw.dim_country
    ADD CONSTRAINT chk_dim_country_current_period
    CHECK (
        (is_current = TRUE AND effective_to IS NULL)
        OR
        (is_current = FALSE AND effective_to IS NOT NULL)
    );

-- SCD Type 2 requires multiple historical rows for the same business key.
-- The original schema enforced one row per country_id, so that constraint
-- must be removed before historical versions can coexist.
ALTER TABLE dw.dim_country
DROP CONSTRAINT dim_country_country_id_key;

CREATE UNIQUE INDEX uq_dim_country_current_country_id
ON dw.dim_country (country_id)
WHERE is_current = TRUE;

CREATE INDEX ix_dim_country_country_id_effective_period
ON dw.dim_country (
    country_id,
    effective_from,
    effective_to
);
