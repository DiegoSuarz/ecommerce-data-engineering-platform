from db import get_postgres_connection

def start_etl_run(pipeline_name):
    query = """
        INSERT INTO audit.etl_run
        (
            pipeline_name,
            status
        )
        VALUES (%s, 'RUNNING')
        RETURNING run_id;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (pipeline_name,))
            run_id = cursor.fetchone()[0]

        connection.commit()

    return run_id



def complete_etl_run(
    run_id,
    rows_extracted,
    rows_loaded,
    rows_rejected=0,
):
    query = """
        UPDATE audit.etl_run
        SET
            finished_at = CURRENT_TIMESTAMP,
            status = 'SUCCESS',
            rows_extracted = %s,
            rows_loaded = %s,
            rows_rejected = %s
        WHERE run_id = %s;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    rows_extracted,
                    rows_loaded,
                    rows_rejected,
                    run_id,
                ),
            )

        connection.commit()

def fail_etl_run(run_id, error_message):
    query = """
        UPDATE audit.etl_run
        SET
            finished_at = CURRENT_TIMESTAMP,
            status = 'FAILED',
            error_message = %s
        WHERE run_id = %s;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    error_message,
                    run_id,
                ),
            )

        connection.commit()


def get_watermark(
    pipeline_name,
    watermark_name,
):
    query = """
        SELECT watermark_value
        FROM audit.pipeline_watermark
        WHERE pipeline_name = %s
          AND watermark_name = %s;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    pipeline_name,
                    watermark_name,
                ),
            )

            row = cursor.fetchone()

    if row is None:
        return None

    return row[0]


def initialize_watermark(
    pipeline_name,
    watermark_name,
):
    query = """
        INSERT INTO audit.pipeline_watermark
        (
            pipeline_name,
            watermark_name,
            watermark_value
        )
        SELECT
            %s,
            %s,
            COALESCE(MAX(order_id), 0)
        FROM dw.fact_sales
        ON CONFLICT (pipeline_name, watermark_name)
        DO NOTHING
        RETURNING watermark_value;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    pipeline_name,
                    watermark_name,
                ),
            )

            row = cursor.fetchone()

        connection.commit()

    if row is None:
        return get_watermark(
            pipeline_name,
            watermark_name,
        )

    return row[0]


def update_watermark(
    pipeline_name,
    watermark_name,
    watermark_value,
):
    query = """
        UPDATE audit.pipeline_watermark
        SET
            watermark_value = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE pipeline_name = %s
          AND watermark_name = %s;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    watermark_value,
                    pipeline_name,
                    watermark_name,
                ),
            )

        connection.commit()
