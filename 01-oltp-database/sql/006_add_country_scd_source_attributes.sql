USE sales;

ALTER TABLE countries
    ADD COLUMN country_code VARCHAR(2) NULL,
    ADD COLUMN sales_region VARCHAR(50) NULL,
    ADD COLUMN market_segment VARCHAR(20) NULL,
    ADD COLUMN updated_at TIMESTAMP(6) NULL;


UPDATE countries
SET country_code = CASE country_id
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
END
WHERE country_id BETWEEN 1 AND 56;


UPDATE countries
SET sales_region = CASE
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
END
WHERE country_id BETWEEN 1 AND 56;


UPDATE countries
SET market_segment = CASE MOD(country_id, 3)
    WHEN 0 THEN 'STANDARD'
    WHEN 1 THEN 'GROWTH'
    WHEN 2 THEN 'STRATEGIC'
END
WHERE country_id BETWEEN 1 AND 56;

UPDATE countries
SET updated_at = CURRENT_TIMESTAMP(6)
WHERE country_id BETWEEN 1 AND 56;

ALTER TABLE countries
    MODIFY COLUMN country_code VARCHAR(2) NOT NULL,
    MODIFY COLUMN sales_region VARCHAR(50) NOT NULL,
    MODIFY COLUMN market_segment VARCHAR(20) NOT NULL,
    MODIFY COLUMN updated_at TIMESTAMP(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    ADD CONSTRAINT uq_countries_code UNIQUE (country_code),
    ADD INDEX ix_countries_updated_at_country_id (updated_at, country_id);
