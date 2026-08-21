from db import get_mysql_connection


def extract_categories():
    query = """
        SELECT
            category_id,
            category_name
        FROM categories
        ORDER BY category_id;
    """

    with get_mysql_connection() as connection:
        cursor = connection.cursor()

        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        finally:
            cursor.close()

    return rows


def extract_countries():
    query = """
        SELECT
            country_id,
            country_name
        FROM countries
        ORDER BY country_id;
    """

    with get_mysql_connection() as connection:
        cursor = connection.cursor()

        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        finally:
            cursor.close()

    return rows


def extract_orders(batch_size=10000):
    query = """
        SELECT
            order_id,
            order_date,
            country_id,
            category_id,
            amount,
            updated_at
        FROM orders
        ORDER BY order_id;
    """

    with get_mysql_connection() as connection:
        cursor = connection.cursor()

        try:
            cursor.execute(query)

            while True:
                rows = cursor.fetchmany(batch_size)

                if not rows:
                    break

                yield rows

        finally:
            if connection.unread_result:
                connection.consume_results()

            cursor.close()



def extract_incremental_orders(
    watermark_timestamp,
    watermark_order_id,
    batch_size=10000,
):
    query = """
        SELECT
            order_id,
            order_date,
            country_id,
            category_id,
            amount,
            updated_at
        FROM orders
        WHERE
            updated_at > %s
            OR (
                updated_at = %s
                AND order_id > %s
                )
        ORDER BY
            updated_at,
            order_id;
    """

    with get_mysql_connection() as connection:
        cursor = connection.cursor()

        try:
            cursor.execute(
                query,
                (
                    watermark_timestamp,
                    watermark_timestamp,
                    watermark_order_id
                ),
            )

            while True:
                rows = cursor.fetchmany(batch_size)

                if not rows:
                    break

                yield rows

        finally:
            if connection.unread_result:
                connection.consume_results()

            cursor.close()
