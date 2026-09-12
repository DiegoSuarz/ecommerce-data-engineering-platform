from audit import upsert_cdc_checkpoint
from db import get_postgres_connection
from load import insert_change_events
from logger import get_logger
import os

from audit import (
    complete_cdc_batch,
    complete_etl_run,
    fail_etl_run,
    get_cdc_checkpoint,
    initialize_cdc_checkpoint,
    start_cdc_batch,
    start_etl_run,
    upsert_cdc_checkpoint,
    update_cdc_checkpoint,
)

from cdc import (
    build_primary_key_columns_by_table,
    create_cdc_stream,
    get_current_binlog_coordinate,
    is_binlog_coordinate_ahead,
    iter_committed_transactions,
)

from db import (
    get_mysql_cdc_settings,
    get_postgres_connection,
)
from load import insert_change_events
from logger import get_logger


logger = get_logger("cdc_load")

PIPELINE_NAME = "change_data_capture"
CHECKPOINT_NAME = "mysql_sales_binlog"
SOURCE_SCHEMA = "sales"
DEFAULT_CDC_SERVER_ID = 100

TRANSFORMED_EVENT_COLUMNS = (
    "transformed_event_id",
    "batch_id",
    "raw_event_id",
    "event_key",
    "operation",
    "source_schema",
    "source_table",
    "primary_key",
    "before_values",
    "after_values",
    "binlog_file",
    "event_end_position",
    "row_index",
    "transaction_id",
    "commit_position",
    "event_timestamp",
    "commit_timestamp",
)

def persist_cdc_transaction(
    batch_id,
    transaction,
    pipeline_name,
    checkpoint_name,
):
    change_events = transaction[
        "change_events"
    ]

    binlog_file = transaction[
        "binlog_file"
    ]

    commit_position = transaction[
        "commit_position"
    ]

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                rows_inserted = (
                    insert_change_events(
                        cursor,
                        batch_id,
                        change_events,
                    )
                )

                upsert_cdc_checkpoint(
                    cursor,
                    pipeline_name,
                    checkpoint_name,
                    binlog_file,
                    commit_position,
                )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

    logger.info(
        "CDC transaction persisted. "
        "transaction_id=%s "
        "events_received=%s "
        "events_inserted=%s "
        "checkpoint=%s:%s",
        transaction["transaction_id"],
        len(change_events),
        rows_inserted,
        binlog_file,
        commit_position,
    )

    return rows_inserted

def run_cdc_batch(
    orchestrator=None,
    orchestrator_run_id=None,
    orchestrator_task_id=None,
    orchestrator_try_number=None,
):
    run_id = start_etl_run(
        PIPELINE_NAME,
        orchestrator=orchestrator,
        orchestrator_run_id=orchestrator_run_id,
        orchestrator_task_id=orchestrator_task_id,
        orchestrator_try_number=(
            orchestrator_try_number
        ),
    )

    try:
        cdc_settings = (
            get_mysql_cdc_settings()
        )

        checkpoint = get_cdc_checkpoint(
            PIPELINE_NAME,
            CHECKPOINT_NAME,
        )

        if checkpoint is None:
            current_coordinate = (
                get_current_binlog_coordinate(
                    cdc_settings
                )
            )

            checkpoint = (
                initialize_cdc_checkpoint(
                    PIPELINE_NAME,
                    CHECKPOINT_NAME,
                    current_coordinate[0],
                    current_coordinate[1],
                )
            )

        (
            start_binlog_file,
            start_binlog_position,
        ) = checkpoint

        batch_id = start_cdc_batch(
            run_id=run_id,
            start_binlog_file=(
                start_binlog_file
            ),
            start_binlog_position=(
                start_binlog_position
            ),
        )

        primary_key_columns_by_table = (
            build_primary_key_columns_by_table(
                SOURCE_SCHEMA
            )
        )

        server_id = int(
            os.getenv(
                "MYSQL_CDC_SERVER_ID",
                DEFAULT_CDC_SERVER_ID,
            )
        )

        stream = create_cdc_stream(
            connection_settings=cdc_settings,
            source_schema=SOURCE_SCHEMA,
            log_file=start_binlog_file,
            log_position=start_binlog_position,
            server_id=server_id,
        )

        transactions_processed = 0
        events_processed = 0
        events_inserted = 0

        insert_events = 0
        update_events = 0
        delete_events = 0

        end_binlog_file = (
            start_binlog_file
        )
        end_binlog_position = (
            start_binlog_position
        )

        safe_read_coordinate = None

        try:
            for transaction in (
                iter_committed_transactions(
                    stream,
                    primary_key_columns_by_table,
                )
            ):
                rows_inserted = (
                    persist_cdc_transaction(
                        batch_id=batch_id,
                        transaction=transaction,
                        pipeline_name=PIPELINE_NAME,
                        checkpoint_name=(
                            CHECKPOINT_NAME
                        ),
                    )
                )

                transactions_processed += 1
                events_processed += len(
                    transaction["change_events"]
                )
                events_inserted += rows_inserted

                for change_event in (
                    transaction["change_events"]
                ):
                    operation = change_event[
                        "operation"
                    ]

                    if operation == "INSERT":
                        insert_events += 1
                    elif operation == "UPDATE":
                        update_events += 1
                    elif operation == "DELETE":
                        delete_events += 1

                end_binlog_file = transaction[
                    "binlog_file"
                ]
                end_binlog_position = transaction[
                    "commit_position"
                ]

            # This code is reached ONLY after normal
            # exhaustion of the non-blocking stream.
            safe_read_coordinate = (
                stream.log_file,
                stream.log_pos,
            )

        finally:
            stream.close()

        if (
            safe_read_coordinate
            != (
                end_binlog_file,
                end_binlog_position,
            )
        ):
            (
                safe_read_file,
                safe_read_position,
            ) = safe_read_coordinate

            update_cdc_checkpoint(
                PIPELINE_NAME,
                CHECKPOINT_NAME,
                safe_read_file,
                safe_read_position,
            )

            end_binlog_file = safe_read_file
            end_binlog_position = (
                safe_read_position
            )

            logger.info(
                "CDC checkpoint advanced to "
                "safe read cursor. "
                "checkpoint=%s:%s",
                end_binlog_file,
                end_binlog_position,
            )

        complete_cdc_batch(
            batch_id=batch_id,
            end_binlog_file=end_binlog_file,
            end_binlog_position=(
                end_binlog_position
            ),
            transactions_processed=(
                transactions_processed
            ),
            events_processed=events_processed,
            insert_events=insert_events,
            update_events=update_events,
            delete_events=delete_events,
        )

        complete_etl_run(
            run_id,
            rows_extracted=events_processed,
            rows_loaded=events_inserted,
            rows_rejected=0,
        )

        logger.info(
            "CDC batch completed successfully. "
            "transactions=%s "
            "events=%s "
            "inserted=%s "
            "checkpoint=%s:%s",
            transactions_processed,
            events_processed,
            events_inserted,
            end_binlog_file,
            end_binlog_position,
        )

        return {
            "run_id": run_id,
            "batch_id": batch_id,
            "transactions_processed": (
                transactions_processed
            ),
            "events_processed": (
                events_processed
            ),
            "events_inserted": (
                events_inserted
            ),
            "insert_events": insert_events,
            "update_events": update_events,
            "delete_events": delete_events,
            "end_binlog_file": (
                end_binlog_file
            ),
            "end_binlog_position": (
                end_binlog_position
            ),
        }

    except Exception as exc:
        fail_etl_run(
            run_id,
            str(exc),
        )

        logger.exception(
            "CDC batch failed."
        )

        raise

def fetch_transformed_events(
    cursor,
    batch_id,
):
    cursor.execute(
        """
        SELECT
            transformed_event_id,
            batch_id,
            raw_event_id,
            event_key,
            operation,
            source_schema,
            source_table,
            primary_key,
            before_values,
            after_values,
            binlog_file,
            event_end_position,
            row_index,
            transaction_id,
            commit_position,
            event_timestamp,
            commit_timestamp
        FROM cdc.transformed_event
        WHERE batch_id = %s
        ORDER BY transformed_event_id
        """,
        (batch_id,),
    )

    return [
        dict(
            zip(
                TRANSFORMED_EVENT_COLUMNS,
                row,
            )
        )
        for row in cursor.fetchall()
    ]

def validate_batch_ready_for_load(
    cursor,
    batch_id,
):
    cursor.execute(
        """
        SELECT
            (
                SELECT COUNT(*)
                FROM cdc.raw_change_event
                WHERE batch_id = %s
            ) AS raw_count,
            (
                SELECT COUNT(*)
                FROM cdc.transformed_event
                WHERE batch_id = %s
            ) AS transformed_count
        """,
        (
            batch_id,
            batch_id,
        ),
    )

    raw_count, transformed_count = (
        cursor.fetchone()
    )

    if raw_count != transformed_count:
        raise RuntimeError(
            "CDC batch is not ready for LOAD: "
            f"batch_id={batch_id} "
            f"raw={raw_count} "
            f"transformed={transformed_count}."
        )

    return {
        "raw_events": raw_count,
        "transformed_events": (
            transformed_count
        ),
    }

def load_transformed_cdc_batch(
    batch_id,
    end_binlog_file,
    end_binlog_position,
    pipeline_name=PIPELINE_NAME,
    checkpoint_name=CHECKPOINT_NAME,
):
    """
    Load one fully transformed CDC batch.

    Final ChangeEvents and the APPLY
    checkpoint are committed atomically.
    """

    target_coordinate = (
        end_binlog_file,
        end_binlog_position,
    )

    current_checkpoint = (
        get_cdc_checkpoint(
            pipeline_name,
            checkpoint_name,
        )
    )

    if current_checkpoint is None:
        raise RuntimeError(
            "CDC APPLY checkpoint does "
            "not exist."
        )

    if (
        target_coordinate
        != current_checkpoint
        and not is_binlog_coordinate_ahead(
            target_coordinate,
            current_checkpoint,
        )
    ):
        raise RuntimeError(
            "CDC APPLY checkpoint "
            "regression rejected: "
            f"current={current_checkpoint} "
            f"target={target_coordinate}."
        )

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                validation = (
                    validate_batch_ready_for_load(
                        cursor=cursor,
                        batch_id=batch_id,
                    )
                )

                transformed_events = (
                    fetch_transformed_events(
                        cursor=cursor,
                        batch_id=batch_id,
                    )
                )

                rows_inserted = (
                    insert_change_events(
                        cursor=cursor,
                        batch_id=batch_id,
                        change_events=(
                            transformed_events
                        ),
                    )
                )

                upsert_cdc_checkpoint(
                    cursor,
                    pipeline_name,
                    checkpoint_name,
                    end_binlog_file,
                    end_binlog_position,
                )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

    result = {
        "batch_id": batch_id,
        "raw_events": validation[
            "raw_events"
        ],
        "transformed_events": validation[
            "transformed_events"
        ],
        "events_loaded": len(
            transformed_events
        ),
        "events_inserted": rows_inserted,
        "end_binlog_file": (
            end_binlog_file
        ),
        "end_binlog_position": (
            end_binlog_position
        ),
    }

    logger.info(
        "CDC LOAD completed. "
        "batch_id=%s events=%s "
        "inserted=%s "
        "apply_checkpoint=%s:%s",
        batch_id,
        len(transformed_events),
        rows_inserted,
        end_binlog_file,
        end_binlog_position,
    )

    return result

def get_transformed_batch_metrics(
    batch_id,
):
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(
                        DISTINCT (
                            binlog_file,
                            transaction_id,
                            commit_position
                        )
                    ),
                    COUNT(*),
                    COUNT(*) FILTER (
                        WHERE operation = 'INSERT'
                    ),
                    COUNT(*) FILTER (
                        WHERE operation = 'UPDATE'
                    ),
                    COUNT(*) FILTER (
                        WHERE operation = 'DELETE'
                    )
                FROM cdc.transformed_event
                WHERE batch_id = %s
                """,
                (batch_id,),
            )

            (
                transaction_count,
                event_count,
                insert_count,
                update_count,
                delete_count,
            ) = cursor.fetchone()

    return {
        "transaction_count": (
            transaction_count
        ),
        "event_count": event_count,
        "insert_count": insert_count,
        "update_count": update_count,
        "delete_count": delete_count,
    }

if __name__ == "__main__":
    run_cdc_batch()
