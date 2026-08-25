from datetime import datetime, timezone
from db import get_postgres_connection

def start_etl_run(
    pipeline_name,
    orchestrator=None,
    orchestrator_run_id=None,
    orchestrator_task_id=None,
    orchestrator_try_number=None,
):
    query = """
    INSERT INTO audit.etl_run
    (
        pipeline_name,
        status,
        orchestrator,
        orchestrator_run_id,
        orchestrator_task_id,
        orchestrator_try_number
    )
    VALUES (%s, 'RUNNING', %s, %s, %s, %s)
    RETURNING run_id;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    pipeline_name,
                    orchestrator,
                    orchestrator_run_id,
                    orchestrator_task_id,
                    orchestrator_try_number,
                ),
)
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
        SELECT
            watermark_timestamp,
            watermark_order_id
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

    return row[0], row[1]


def initialize_watermark(
        pipeline_name,
        watermark_name,
):

    watermark_timestamp = datetime.min.replace(
        tzinfo=timezone.utc
    )
    watermark_order_id = 0

    select_query = """
        SELECT
            source_updated_at,
            order_id
        FROM dw.fact_sales
        ORDER BY
            source_updated_at DESC,
            order_id DESC
        LIMIT 1;
    """
    insert_query = """
        INSERT INTO audit.pipeline_watermark
        (
            pipeline_name,
            watermark_name,
            watermark_timestamp,
            watermark_order_id
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (pipeline_name, watermark_name)
        DO NOTHING;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(select_query)
            row = cursor.fetchone()
            if row is not None:
                watermark_timestamp = row[0]
                watermark_order_id = row[1]
            cursor.execute(
                insert_query,
                (
                    pipeline_name,
                    watermark_name,
                    watermark_timestamp,
                    watermark_order_id,
                ),
            )
        connection.commit()

    watermark = get_watermark(
        pipeline_name,
        watermark_name,
    )
    return watermark


def update_watermark(
    pipeline_name,
    watermark_name,
    watermark_timestamp,
    watermark_order_id,
):
    query = """
        UPDATE audit.pipeline_watermark
        SET
            watermark_timestamp = %s,
            watermark_order_id = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE pipeline_name = %s
          AND watermark_name = %s;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    watermark_timestamp,
                    watermark_order_id,
                    pipeline_name,
                    watermark_name,
                ),
            )

        connection.commit()

def mark_stale_etl_runs(
    pipeline_name,
    stale_after_minutes=15,
):
    query = """
    UPDATE audit.etl_run
    SET
        finished_at = CURRENT_TIMESTAMP,
        status = 'FAILED',
        error_message =
            'Marked as stale: execution ended without final audit status'
    WHERE pipeline_name = %s
      AND status = 'RUNNING'
      AND started_at <
          CURRENT_TIMESTAMP
          - (%s * INTERVAL '1 minute')
    RETURNING run_id;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    pipeline_name,
                    stale_after_minutes,
                ),
            )

            rows = cursor.fetchall()

        connection.commit()

    return [row[0] for row in rows]
