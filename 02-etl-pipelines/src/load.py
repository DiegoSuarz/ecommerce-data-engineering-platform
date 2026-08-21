from db import get_postgres_connection
from logger import get_logger

logger = get_logger("load")

def load_categories(rows):
    insert_query = """
        INSERT INTO staging.categories
        (
            category_id,
            category_name
        )
        VALUES (%s, %s);
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
            country_name
        )
        VALUES (%s, %s);
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


def load_dim_category():
    query = """
        INSERT INTO dw.dim_category
        (
            category_id,
            category_name
        )
        SELECT
            category_id,
            category_name
        FROM staging.categories
        ORDER BY category_id;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE dw.dim_category "
                "RESTART IDENTITY CASCADE;"
            )
            cursor.execute(query)

        connection.commit()


def load_dim_country():
    query = """
        INSERT INTO dw.dim_country
        (
            country_id,
            country_name
        )
        SELECT
            country_id,
            country_name
        FROM staging.countries
        ORDER BY country_id;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE dw.dim_country "
                "RESTART IDENTITY CASCADE;"
            )
            cursor.execute(query)

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

        INNER JOIN dw.dim_category cat
            ON o.category_id = cat.category_id

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

        INNER JOIN dw.dim_category cat
            ON o.category_id = cat.category_id

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
