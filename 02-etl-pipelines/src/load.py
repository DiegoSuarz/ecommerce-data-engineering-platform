from db import get_postgres_connection
from logger import get_logger

logger = get_logger("load")

def load_categories(rows):
    insert_query = """
        INSERT INTO staging.categories
        (
            category_id,
            category_code,
            category_name,
            department,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s);
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE staging.categories;")
            cursor.executemany(insert_query, rows)

        connection.commit()

def load_countries(rows):
    insert_query = """
        INSERT INTO staging.countries
        (
            country_id,
            country_code,
            country_name,
            sales_region,
            market_segment,
            updated_at,
            row_hash
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE staging.countries;")
            cursor.executemany(insert_query, rows)

        connection.commit()

def load_orders(order_batches):
    insert_query = """
        INSERT INTO staging.orders
        (
            order_id,
            order_date,
            country_id,
            category_id,
            amount,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s);
    """

    total_loaded = 0
    batch_number = 0

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE staging.orders;")

            for batch in order_batches:
                batch_number += 1

                cursor.executemany(
                    insert_query,
                    batch,
                )

                total_loaded += len(batch)

                logger.info(
                    "Orders batch %s loaded: rows=%s total=%s",
                    batch_number,
                    len(batch),
                    total_loaded,
                )

        connection.commit()

    return total_loaded


def load_dim_date(rows):
    insert_query = """
        INSERT INTO dw.dim_date
        (
            date_key,
            full_date,
            year,
            quarter,
            quarter_name,
            month,
            month_name,
            day,
            weekday,
            weekday_name
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE dw.dim_date CASCADE;")
            cursor.executemany(insert_query, rows)

        connection.commit()


def load_dim_category(initial_effective_from=None):
    insert_query = """
        INSERT INTO dw.dim_category
        (
            category_id,
            category_code,
            category_name,
            department,
            effective_from,
            effective_to,
            is_current
        )
        SELECT
            s.category_id,
            s.category_code,
            s.category_name,
            s.department,
            COALESCE(%s, s.updated_at),
            NULL,
            TRUE
        FROM staging.categories AS s
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM dw.dim_category AS d
            WHERE d.category_id = s.category_id
        )
        ORDER BY s.category_id;
    """

    type2_update_query = """
    WITH changed_categories AS
    (
        UPDATE dw.dim_category AS d
        SET
            effective_to = s.updated_at,
            is_current = FALSE
        FROM staging.categories AS s
        WHERE d.category_id = s.category_id
          AND d.is_current = TRUE
          AND d.department IS DISTINCT FROM s.department
          AND s.updated_at > d.effective_from
        RETURNING
            d.category_id,
            d.category_code
    )
    INSERT INTO dw.dim_category
    (
        category_id,
        category_code,
        category_name,
        department,
        effective_from,
        effective_to,
        is_current
    )
    SELECT
        c.category_id,
        c.category_code,
        s.category_name,
        s.department,
        s.updated_at,
        NULL,
        TRUE
    FROM changed_categories AS c
    INNER JOIN staging.categories AS s
        ON c.category_id = s.category_id;
    """

    type1_update_query = """
        UPDATE dw.dim_category AS d
        SET
            category_name = s.category_name
        FROM staging.categories AS s
        WHERE d.category_id = s.category_id
          AND d.category_name IS DISTINCT FROM s.category_name;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                insert_query,
                (initial_effective_from,),
            )
            cursor.execute(type2_update_query)
            cursor.execute(type1_update_query)

        connection.commit()

def load_dim_country(initial_effective_from=None):
    insert_query = """
        INSERT INTO dw.dim_country
        (
            country_id,
            country_code,
            country_name,
            sales_region,
            market_segment,
            row_hash,
            effective_from,
            effective_to,
            is_current
        )
        SELECT
            s.country_id,
            s.country_code,
            s.country_name,
            s.sales_region,
            s.market_segment,
            s.row_hash,
            COALESCE(%s, s.updated_at),
            NULL,
            TRUE
        FROM staging.countries AS s
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM dw.dim_country AS d
            WHERE d.country_id = s.country_id
        )
        ORDER BY s.country_id;
    """

    initialize_hash_query = """
    UPDATE dw.dim_country AS d
    SET
        row_hash = s.row_hash
    FROM staging.countries AS s
    WHERE d.country_id = s.country_id
      AND d.is_current = TRUE
      AND d.row_hash IS NULL
      AND d.sales_region IS NOT DISTINCT FROM s.sales_region
      AND d.market_segment IS NOT DISTINCT FROM s.market_segment;
    """

    type2_update_query = """
        WITH changed_countries AS
        (
            UPDATE dw.dim_country AS d
            SET
                effective_to = s.updated_at,
                is_current = FALSE
            FROM staging.countries AS s
            WHERE d.country_id = s.country_id
              AND d.is_current = TRUE
              AND d.row_hash IS DISTINCT FROM s.row_hash
              AND s.updated_at > d.effective_from
            RETURNING
                d.country_id,
                d.country_code
        )
        INSERT INTO dw.dim_country
        (
            country_id,
            country_code,
            country_name,
            sales_region,
            market_segment,
            row_hash,
            effective_from,
            effective_to,
            is_current
        )
        SELECT
            c.country_id,
            c.country_code,
            s.country_name,
            s.sales_region,
            s.market_segment,
            s.row_hash,
            s.updated_at,
            NULL,
            TRUE
        FROM changed_countries AS c
        INNER JOIN staging.countries AS s
            ON c.country_id = s.country_id;
    """

    type1_update_query = """
        UPDATE dw.dim_country AS d
        SET
            country_name = s.country_name
        FROM staging.countries AS s
        WHERE d.country_id = s.country_id
          AND d.country_name IS DISTINCT FROM s.country_name;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                insert_query,
                (initial_effective_from,),
            )
            cursor.execute(initialize_hash_query)
            cursor.execute(type2_update_query)
            cursor.execute(type1_update_query)

        connection.commit()



def load_fact_sales():
    query = """
        INSERT INTO dw.fact_sales
        (
            order_id,
            date_key,
            country_key,
            category_key,
            amount,
            source_updated_at
        )
        SELECT
            o.order_id,
            d.date_key,
            c.country_key,
            cat.category_key,
            o.amount,
            o.updated_at
        FROM staging.orders o

        INNER JOIN dw.dim_date d
            ON o.order_date = d.full_date

        INNER JOIN dw.dim_country c
            ON o.country_id = c.country_id
            AND (o.order_date::timestamp AT TIME ZONE 'UTC') >= c.effective_from
            AND (
                c.effective_to IS NULL
                OR (o.order_date::timestamp AT TIME ZONE 'UTC') < c.effective_to
            )

        INNER JOIN dw.dim_category cat
            ON o.category_id = cat.category_id
            AND (o.order_date::timestamp AT TIME ZONE 'UTC') >= cat.effective_from
            AND (
                cat.effective_to IS NULL
                OR (o.order_date::timestamp AT TIME ZONE 'UTC') < cat.effective_to
            )

        ORDER BY o.order_id;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE dw.fact_sales "
                "RESTART IDENTITY;"
            )
            cursor.execute(query)

        connection.commit()


def load_incremental_orders(order_batches):
    insert_query = """
        INSERT INTO staging.orders
        (
            order_id,
            order_date,
            country_id,
            category_id,
            amount,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s);
    """

    total_loaded = 0
    max_watermark = None

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE staging.orders;")

            for batch_number, batch in enumerate(
                order_batches,
                start=1,
            ):
                if not batch:
                    continue

                cursor.executemany(
                    insert_query,
                    batch,
                )

                total_loaded += len(batch)

                batch_max_watermark = max(
                    (row[5], row[0])
                    for row in batch
                )

                if (
                    max_watermark is None
                    or batch_max_watermark > max_watermark
                ):
                    max_watermark = batch_max_watermark

                logger.info(
                    "Incremental orders batch %s loaded: "
                    "rows=%s total=%s",
                    batch_number,
                    len(batch),
                    total_loaded,
                )

        connection.commit()

    return total_loaded, max_watermark


def load_incremental_dim_date(rows):
    query = """
        INSERT INTO dw.dim_date
        (
            date_key,
            full_date,
            year,
            quarter,
            quarter_name,
            month,
            month_name,
            day,
            weekday,
            weekday_name
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_key)
        DO NOTHING;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.executemany(query, rows)

        connection.commit()


def load_incremental_fact_sales():
    query = """
        INSERT INTO dw.fact_sales
        (
            order_id,
            date_key,
            country_key,
            category_key,
            amount,
            source_updated_at
        )
        SELECT
            o.order_id,
            d.date_key,
            c.country_key,
            cat.category_key,
            o.amount,
            o.updated_at
        FROM staging.orders o

        INNER JOIN dw.dim_date d
            ON o.order_date = d.full_date

        INNER JOIN dw.dim_country c
            ON o.country_id = c.country_id
            AND (o.order_date::timestamp AT TIME ZONE 'UTC') >= c.effective_from
            AND (
                c.effective_to IS NULL
                OR (o.order_date::timestamp AT TIME ZONE 'UTC') < c.effective_to
            )

        INNER JOIN dw.dim_category cat
            ON o.category_id = cat.category_id
            AND (o.order_date::timestamp AT TIME ZONE 'UTC') >= cat.effective_from
            AND (
                cat.effective_to IS NULL
                OR (o.order_date::timestamp AT TIME ZONE 'UTC') < cat.effective_to
            )

        ON CONFLICT (order_id)
        DO UPDATE
        SET
            date_key = EXCLUDED.date_key,
            country_key = EXCLUDED.country_key,
            category_key = EXCLUDED.category_key,
            amount = EXCLUDED.amount,
            source_updated_at = EXCLUDED.source_updated_at;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)

        connection.commit()
