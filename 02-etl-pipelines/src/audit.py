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
        ON CONFLICT
        (
            pipeline_name,
            orchestrator,
            orchestrator_run_id
        )
        WHERE orchestrator IS NOT NULL
        AND orchestrator_run_id IS NOT NULL
        DO UPDATE
        SET
            orchestrator_task_id =
                EXCLUDED.orchestrator_task_id,
            orchestrator_try_number =
                EXCLUDED.orchestrator_try_number
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

def get_cdc_checkpoint(
    pipeline_name,
    checkpoint_name,
):
    query = """
        SELECT
            binlog_file,
            binlog_position
        FROM audit.cdc_checkpoint
        WHERE pipeline_name = %s
          AND checkpoint_name = %s;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    pipeline_name,
                    checkpoint_name,
                ),
            )

            row = cursor.fetchone()

    if row is None:
        return None

    return row[0], row[1]


def initialize_cdc_checkpoint(
    pipeline_name,
    checkpoint_name,
    binlog_file,
    binlog_position,
):
    query = """
        INSERT INTO audit.cdc_checkpoint
        (
            pipeline_name,
            checkpoint_name,
            binlog_file,
            binlog_position
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (
            pipeline_name,
            checkpoint_name
        )
        DO NOTHING;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    pipeline_name,
                    checkpoint_name,
                    binlog_file,
                    binlog_position,
                ),
            )

        connection.commit()

    return get_cdc_checkpoint(
        pipeline_name,
        checkpoint_name,
    )

def initialize_cdc_checkpoint_from_checkpoint(
    pipeline_name,
    source_checkpoint_name,
    target_checkpoint_name,
):
    """
    Initialize a CDC checkpoint from another checkpoint.

    The target checkpoint is created only when it does not
    already exist. This makes the operation idempotent.

    The source checkpoint is used only for the initial
    bootstrap of the target.
    """

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO audit.cdc_checkpoint (
                        pipeline_name,
                        checkpoint_name,
                        binlog_file,
                        binlog_position,
                        updated_at
                    )
                    SELECT
                        pipeline_name,
                        %s,
                        binlog_file,
                        binlog_position,
                        CURRENT_TIMESTAMP
                    FROM audit.cdc_checkpoint
                    WHERE pipeline_name = %s
                      AND checkpoint_name = %s
                    ON CONFLICT (
                        pipeline_name,
                        checkpoint_name
                    )
                    DO NOTHING
                    RETURNING
                        binlog_file,
                        binlog_position
                    """,
                    (
                        target_checkpoint_name,
                        pipeline_name,
                        source_checkpoint_name,
                    ),
                )

                checkpoint = cursor.fetchone()

                if checkpoint is None:
                    cursor.execute(
                        """
                        SELECT
                            binlog_file,
                            binlog_position
                        FROM audit.cdc_checkpoint
                        WHERE pipeline_name = %s
                          AND checkpoint_name = %s
                        """,
                        (
                            pipeline_name,
                            target_checkpoint_name,
                        ),
                    )

                    checkpoint = cursor.fetchone()

                if checkpoint is None:
                    raise RuntimeError(
                        "Unable to initialize CDC "
                        "checkpoint "
                        f"{target_checkpoint_name!r} "
                        "from "
                        f"{source_checkpoint_name!r}: "
                        "source checkpoint does not "
                        "exist."
                    )

            connection.commit()

            return (
                checkpoint[0],
                checkpoint[1],
            )

        except Exception:
            connection.rollback()
            raise

def upsert_cdc_checkpoint(
    cursor,
    pipeline_name,
    checkpoint_name,
    binlog_file,
    binlog_position,
):
    query = """
        INSERT INTO audit.cdc_checkpoint
        (
            pipeline_name,
            checkpoint_name,
            binlog_file,
            binlog_position
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (
            pipeline_name,
            checkpoint_name
        )
        DO UPDATE
        SET
            binlog_file = EXCLUDED.binlog_file,
            binlog_position =
                EXCLUDED.binlog_position,
            updated_at = CURRENT_TIMESTAMP;
    """

    cursor.execute(
        query,
        (
            pipeline_name,
            checkpoint_name,
            binlog_file,
            binlog_position,
        ),
    )

def update_cdc_checkpoint(
    pipeline_name,
    checkpoint_name,
    binlog_file,
    binlog_position,
):
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            upsert_cdc_checkpoint(
                cursor,
                pipeline_name,
                checkpoint_name,
                binlog_file,
                binlog_position,
            )

        connection.commit()

def get_latest_cdc_batch_state(
    pipeline_name,
):
    """
    Return the most recent CDC batch and
    its owning ETL run status.

    The ETL run is the lifecycle authority;
    cdc_batch stores CDC coordinates and
    batch metrics.
    """
    query = """
        SELECT
            b.batch_id,
            b.run_id,
            r.status,
            b.start_binlog_file,
            b.start_binlog_position,
            b.end_binlog_file,
            b.end_binlog_position
        FROM audit.cdc_batch AS b
        INNER JOIN audit.etl_run AS r
            ON r.run_id = b.run_id
        WHERE r.pipeline_name = %s
        ORDER BY b.batch_id DESC
        LIMIT 1;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (pipeline_name,),
            )

            row = cursor.fetchone()

    if row is None:
        return None

    return {
        "batch_id": row[0],
        "run_id": row[1],
        "status": row[2],
        "start_binlog_file": row[3],
        "start_binlog_position": row[4],
        "end_binlog_file": row[5],
        "end_binlog_position": row[6],
    }


def start_cdc_batch(
    run_id,
    start_binlog_file,
    start_binlog_position,
):
    query = """
        INSERT INTO audit.cdc_batch
        (
            run_id,
            start_binlog_file,
            start_binlog_position
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (run_id)
        DO NOTHING
        RETURNING batch_id;
    """

    select_query = """
        SELECT batch_id
        FROM audit.cdc_batch
        WHERE run_id = %s;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    run_id,
                    start_binlog_file,
                    start_binlog_position,
                ),
            )

            row = cursor.fetchone()

            if row is None:
                cursor.execute(
                    select_query,
                    (run_id,),
                )
                row = cursor.fetchone()

            batch_id = row[0]

        connection.commit()

    return batch_id

def complete_cdc_batch(
    batch_id,
    end_binlog_file,
    end_binlog_position,
    transactions_processed,
    events_processed,
    insert_events,
    update_events,
    delete_events,
):
    query = """
        UPDATE audit.cdc_batch
        SET
            end_binlog_file = %s,
            end_binlog_position = %s,
            transactions_processed = %s,
            events_processed = %s,
            insert_events = %s,
            update_events = %s,
            delete_events = %s
        WHERE batch_id = %s;
    """

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    end_binlog_file,
                    end_binlog_position,
                    transactions_processed,
                    events_processed,
                    insert_events,
                    update_events,
                    delete_events,
                    batch_id,
                ),
            )

        connection.commit()
