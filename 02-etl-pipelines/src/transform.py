import hashlib

from db import get_postgres_connection


def calculate_scd2_hash(sales_region, market_segment):
    def normalize(value):
        if value is None:
            return "<NULL>"

        return str(value).strip()

    normalized_values = [
        normalize(sales_region),
        normalize(market_segment),
    ]

    canonical_value = "|".join(
        f"{len(value)}:{value}"
        for value in normalized_values
    )

    return hashlib.sha256(
        canonical_value.encode("utf-8")
    ).hexdigest()

def transform_country_rows(rows):
    transformed_rows = []

    for row in rows:
        (
            country_id,
            country_code,
            country_name,
            sales_region,
            market_segment,
            updated_at,
        ) = row

        row_hash = calculate_scd2_hash(
            sales_region,
            market_segment,
        )

        transformed_rows.append(
            (
                country_id,
                country_code,
                country_name,
                sales_region,
                market_segment,
                updated_at,
                row_hash,
            )
        )

    return transformed_rows

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
