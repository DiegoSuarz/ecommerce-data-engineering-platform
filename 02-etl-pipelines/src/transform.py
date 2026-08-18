
from db import get_postgres_connection

def build_date_dimension_rows(order_dates):
    rows = []

    for full_date in sorted(set(order_dates)):
        date_key = int(full_date.strftime("%Y%m%d"))
        quarter = ((full_date.month - 1) // 3) + 1

        rows.append(
            (
                date_key,
                full_date,
                full_date.year,
                quarter,
                f"Q{quarter}",
                full_date.month,
                full_date.strftime("%B"),
                full_date.day,
                full_date.isoweekday(),
                full_date.strftime("%A"),
            )
        )

    return rows




def extract_distinct_order_dates():
    query = """
        SELECT DISTINCT order_date
        FROM staging.orders
        ORDER BY order_date;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    return [row[0] for row in rows]
