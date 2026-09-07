# Slowly Changing Dimensions Design

## 1. Purpose

This document describes the Slowly Changing Dimension (SCD) strategy used in the E-Commerce Data Engineering Platform.

The objective is to define how changes in dimension attributes are handled while preserving the historical context required by the data warehouse.

The implementation applies different SCD behaviors depending on the business meaning of each attribute:

- Type 0 for attributes that must remain unchanged.
- Type 1 for attributes where only the latest value is required.
- Type 2 for attributes where historical changes must be preserved.

## 2. SCD Strategy

### 2.1 Type 0 Attributes

Type 0 attributes are treated as immutable business attributes.

After the initial dimension record is created, changes to these attributes are not applied automatically by the SCD load process. This protects stable business identifiers from accidental modification.

### 2.2 Type 1 Attributes

Type 1 attributes keep only the latest known value.

When a Type 1 attribute changes, the current dimension record is updated in place. No additional historical version is created for that change.

### 2.3 Type 2 Attributes

Type 2 attributes preserve historical changes by creating a new dimension version.

When a Type 2 attribute changes:

1. The current version is closed by setting its `effective_to`.
2. The current version is marked with `is_current = FALSE`.
3. A new dimension version is inserted.
4. The new version starts at the source change timestamp.
5. The new version has `effective_to = NULL` and `is_current = TRUE`.

This allows facts to resolve the dimension version that was valid at the time of the business event.

## 3. Category Dimension

The `dim_category` dimension uses a hybrid SCD strategy.

| Attribute | SCD Type | Behavior |
|---|---|---|
| `category_code` | Type 0 | Treated as an immutable business identifier. |
| `category_name` | Type 1 | Updated in place when the name changes. |
| `department` | Type 2 | Creates a new dimension version when the department changes. |

The source `category_id` is used to identify the business entity across its historical versions.

Type 2 versions are controlled with:

- `effective_from`
- `effective_to`
- `is_current`

This allows the warehouse to preserve changes in department assignment without creating unnecessary historical versions for category name corrections.

## 4. Country Dimension

The `dim_country` dimension also uses a hybrid SCD strategy.

| Attribute | SCD Type | Behavior |
|---|---|---|
| `country_code` | Type 0 | Treated as an immutable business identifier. |
| `country_name` | Type 1 | Updated in place when the name changes. |
| `sales_region` | Type 2 | Creates a new dimension version when the sales region changes. |
| `market_segment` | Type 2 | Creates a new dimension version when the market segment changes. |

The source `country_id` identifies the same business entity across multiple historical versions.

Type 2 changes are tracked with:

- `row_hash`
- `effective_from`
- `effective_to`
- `is_current`

The `row_hash` represents the Type 2 attributes (`sales_region` and `market_segment`) and is used to detect whether a historical change is required.

## 5. Temporal Validity Model

SCD Type 2 versions use half-open temporal intervals.

A dimension version is valid when:

`effective_from <= event_time < effective_to`

For the current version, `effective_to` is `NULL`, which represents an open-ended interval.

When a new Type 2 version is created, the previous version's `effective_to` is set to the same timestamp as the new version's `effective_from`.

Example:

| Version | effective_from | effective_to |
|---|---|---|
| Previous | 2024-01-01 | 2025-06-01 |
| Current | 2025-06-01 | NULL |

An event before `2025-06-01` resolves to the previous version. An event at or after `2025-06-01` resolves to the current version.

This design prevents temporal overlap between consecutive dimension versions.

## 6. SCD2 Hash Strategy

The `dim_country` dimension uses a SHA-256 hash to detect changes in its Type 2 attributes.

The hash includes only:

- `sales_region`
- `market_segment`

Before hashing, each value is normalized. `NULL` values are represented explicitly as `<NULL>`, and non-null values are converted to strings and trimmed.

The normalized values are encoded using their length and value:

`<length>:<value>`

The encoded values are then joined with `|`.

Example:

`sales_region = LATAM`

`market_segment = STRATEGIC`

produces the canonical representation:

`5:LATAM|9:STRATEGIC`

SHA-256 is calculated from this canonical representation and stored in `row_hash`.

During the SCD load, the source hash is compared with the hash of the current dimension version. A different hash indicates that at least one Type 2 attribute changed and a new historical version may be required.

Type 0 and Type 1 attributes are intentionally excluded from this hash because their changes must not create a Type 2 version.

## 7. Fact-to-Dimension Temporal Resolution

Facts are linked to the dimension version that was valid when the business event occurred.

For `fact_sales`, the business event time is derived from `order_date`.

The temporal lookup follows this rule:

`effective_from <= event_time`

and:

`event_time < effective_to OR effective_to IS NULL`

This lookup is applied when resolving the surrogate keys for SCD Type 2 dimensions.

As a result, historical facts remain associated with the dimension version that was valid at the time of the order instead of automatically using the current dimension version.

For this project, `order_date` is stored as a `DATE`. During temporal resolution, it is interpreted as midnight UTC for that date.

## 8. Initial Historical Boundary

During the initial full warehouse load, the first known version of each SCD dimension must cover the historical facts already present in the source data.

Source `updated_at` values cannot always be used as the initial `effective_from`. During a fresh database bootstrap, these timestamps may represent the technical load time rather than the beginning of the historical business validity period.

For this reason, the full load uses:

`1900-01-01 00:00:00 UTC`

as the initial SCD historical boundary.

This value is a technical sentinel. It does not mean that the dimension members actually existed in 1900. It means that the first known warehouse version is considered valid before all business events currently available in the project dataset.

The boundary is applied only when creating the initial dimension versions during the full load.

Incremental processing does not use this sentinel for new dimension members. New members discovered later use their source `updated_at` timestamp as their `effective_from`.

## 9. Database Constraints

The warehouse schema includes database constraints and indexes to protect the integrity of the SCD model independently of the ETL logic.

The main rules are:

- Only one current version of the same business entity is allowed.
- Historical intervals must have a valid temporal order.
- Current versions use `effective_to = NULL`.
- SCD control attributes such as `effective_from` and `is_current` are required.
- `dim_country.row_hash` must contain a valid 64-character SHA-256 hexadecimal value.

Partial unique indexes enforce one current version per business entity while still allowing multiple historical versions with the same source identifier.

For example, multiple rows may share the same `country_id`, but only one of them can have `is_current = TRUE`.

These database protections complement the ETL rules and reduce the risk of creating inconsistent SCD history.

## 10. Migration Strategy

### 10.1 Fresh Deployment

For a fresh warehouse deployment, the PostgreSQL migrations can be applied sequentially from `001` through `012`.

At this point the warehouse and staging tables are still empty, so the SCD constraints can be created before the initial data load.

After the schema is created, the full ETL load populates the dimensions using the initial historical boundary described in this document.

The fresh deployment sequence is:

`001 → ... → 012 → full load`

### 10.2 Existing Populated Database

Upgrading an existing populated warehouse requires an intermediate data backfill step.

Migration `009` introduces the SCD attributes for `dim_country`, and migration `010` introduces the corresponding staging attributes. Existing dimension rows may still have a `NULL` `row_hash` at this stage.

Before applying migration `011`, the ETL must populate staging and initialize the hash of the existing current dimension rows.

The populated upgrade sequence is therefore:

`009 → 010 → ETL/data backfill → 011 → 012`

The ETL initializes a missing `dim_country.row_hash` only when the current dimension Type 2 attributes match the corresponding staging attributes. This avoids interpreting the introduction of the hash column itself as a historical Type 2 change.

Migration `011` then hardens the schema by requiring valid non-null hashes and other SCD constraints.

Migration `012` adds database-level idempotency protection for orchestrated ETL runs.

This intermediate ETL step is an intentional migration boundary for an already populated warehouse.

## 11. Known Limitations

The source model stores `order_date` as a `DATE` and does not provide the exact time when an order occurred.

For temporal SCD resolution, the project interprets `order_date` as midnight UTC.

Because of this limitation, the warehouse cannot determine whether an order occurred before or after a Type 2 dimension change when both events happen on the same calendar day.

For example, if a dimension changes at `2026-09-01 14:00:00 UTC`, an order with `order_date = 2026-09-01` does not contain enough information to determine its exact position relative to that change.

A source timestamp such as `order_timestamp` would be required for precise intra-day temporal resolution.

The current implementation therefore provides deterministic day-level temporal resolution based on the information available in the source system.

## 12. Validation

The SCD implementation is validated through automated tests and fresh-database reproducibility checks.

The automated test suite covers:

- Type 0 attribute behavior.
- Type 1 in-place updates.
- Type 2 historical version creation.
- SCD2 hash calculation and normalization.
- Initial historical boundary behavior.
- Temporal surrogate-key resolution.
- Incremental pipeline behavior.
- Audit and orchestration integration.

Fresh-database validation was also performed by rebuilding the MySQL source and PostgreSQL warehouse from their migration scripts and running the full ETL pipeline.

The fresh full load produced:

- 5 category dimension rows.
- 56 country dimension rows.
- 1,099 date dimension rows.
- 300,005 fact rows.
- 300,066 extracted source rows.
- 301,165 loaded warehouse rows.

All 300,005 fact rows resolved valid category and country dimension versions.

Additional integrity checks confirmed:

- One current version per business entity.
- No invalid temporal intervals.
- No invalid `dim_country` hashes.
- No unresolved temporal fact relationships.

The complete automated test suite passed with 46 tests.
