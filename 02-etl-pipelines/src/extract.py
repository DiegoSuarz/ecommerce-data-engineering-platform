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
            amount
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
    watermark,
    batch_size=10000,
):
    query = """
        SELECT
            order_id,
            order_date,
            country_id,
            category_id,
            amount
        FROM orders
        WHERE order_id > %s
        ORDER BY order_id;
    """

    with get_mysql_connection() as connection:
        cursor = connection.cursor()

        try:
            cursor.execute(
                query,
                (watermark,),
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