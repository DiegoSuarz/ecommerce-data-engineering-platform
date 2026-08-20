from db import get_postgres_connection


def run_staging_quality_checks():
    checks = {
        "duplicate_orders": """
            SELECT COUNT(*)
            FROM
            (
                SELECT order_id
                FROM staging.orders
                GROUP BY order_id
                HAVING COUNT(*) > 1
            ) d;
        """,

        "null_orders": """
            SELECT COUNT(*)
            FROM staging.orders
            WHERE order_id IS NULL
               OR order_date IS NULL
               OR country_id IS NULL
               OR category_id IS NULL
               OR amount IS NULL;
        """,

        "invalid_amounts": """
            SELECT COUNT(*)
            FROM staging.orders
            WHERE amount <= 0;
        """,

        "orphan_countries": """
            SELECT COUNT(*)
            FROM staging.orders o
            LEFT JOIN staging.countries c
                ON o.country_id = c.country_id
            WHERE c.country_id IS NULL;
        """,

        "orphan_categories": """
            SELECT COUNT(*)
            FROM staging.orders o
            LEFT JOIN staging.categories c
                ON o.category_id = c.category_id
            WHERE c.category_id IS NULL;
        """,
    }

    results = {}

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            for check_name, query in checks.items():
                cursor.execute(query)
                result = cursor.fetchone()[0]

                results[check_name] = result

                if result != 0:
                    raise ValueError(
                        f"Data quality check failed: "
                        f"{check_name} = {result}"
                    )

    return results


def run_dw_quality_checks():
    checks = {
        "invalid_date_keys": """
            SELECT COUNT(*)
            FROM dw.fact_sales f
            LEFT JOIN dw.dim_date d
                ON f.date_key = d.date_key
            WHERE d.date_key IS NULL;
        """,

        "invalid_country_keys": """
            SELECT COUNT(*)
            FROM dw.fact_sales f
            LEFT JOIN dw.dim_country c
                ON f.country_key = c.country_key
            WHERE c.country_key IS NULL;
        """,

        "invalid_category_keys": """
            SELECT COUNT(*)
            FROM dw.fact_sales f
            LEFT JOIN dw.dim_category c
                ON f.category_key = c.category_key
            WHERE c.category_key IS NULL;
        """,
    }

    results = {}

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            for check_name, query in checks.items():
                cursor.execute(query)
                result = cursor.fetchone()[0]

                results[check_name] = result

                if result != 0:
                    raise ValueError(
                        f"DW quality check failed: "
                        f"{check_name} = {result}"
                    )

    return results


def reconcile_staging_to_dw():
    query = """
        SELECT
            (SELECT COUNT(*) FROM staging.orders),
            (SELECT COUNT(*) FROM dw.fact_sales),
            (SELECT SUM(amount) FROM staging.orders),
            (SELECT SUM(amount) FROM dw.fact_sales);
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)

            (
                staging_rows,
                dw_rows,
                staging_amount,
                dw_amount,
            ) = cursor.fetchone()

    if staging_rows != dw_rows:
        raise ValueError(
            f"Row reconciliation failed: "
            f"staging={staging_rows}, dw={dw_rows}"
        )

    if staging_amount != dw_amount:
        raise ValueError(
            f"Amount reconciliation failed: "
            f"staging={staging_amount}, dw={dw_amount}"
        )

    return {
        "staging_rows": staging_rows,
        "dw_rows": dw_rows,
        "staging_amount": staging_amount,
        "dw_amount": dw_amount,
    }




def reconcile_incremental_batch():
    query = """
        SELECT
            (SELECT COUNT(*) FROM staging.orders),
            (
                SELECT COUNT(*)
                FROM dw.fact_sales f
                INNER JOIN staging.orders s
                    ON f.order_id = s.order_id
            ),
            (SELECT COALESCE(SUM(amount), 0) FROM staging.orders),
            (
                SELECT COALESCE(SUM(f.amount), 0)
                FROM dw.fact_sales f
                INNER JOIN staging.orders s
                    ON f.order_id = s.order_id
            );
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)

            (
                staging_rows,
                dw_rows,
                staging_amount,
                dw_amount,
            ) = cursor.fetchone()

    if staging_rows != dw_rows:
        raise ValueError(
            f"Incremental row reconciliation failed: "
            f"staging={staging_rows}, dw={dw_rows}"
        )

    if staging_amount != dw_amount:
        raise ValueError(
            f"Incremental amount reconciliation failed: "
            f"staging={staging_amount}, dw={dw_amount}"
        )

    return {
        "staging_rows": staging_rows,
        "dw_rows": dw_rows,
        "staging_amount": staging_amount,
        "dw_amount": dw_amount,
    }