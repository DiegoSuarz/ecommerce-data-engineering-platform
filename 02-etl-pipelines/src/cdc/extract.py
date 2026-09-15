import os

from psycopg.types.json import Jsonb

from audit import (
    get_cdc_checkpoint,
    initialize_cdc_checkpoint_from_checkpoint,
    update_cdc_checkpoint,
    upsert_cdc_checkpoint,
)
from cdc.stream import (
    create_cdc_stream,
    is_binlog_coordinate_ahead,
    iter_raw_committed_transactions,
)
from db import (
    get_mysql_cdc_settings,
    get_postgres_connection,
)
from load import make_json_safe
from logger import get_logger


logger = get_logger("cdc.extract")


PIPELINE_NAME = "change_data_capture"

READ_CHECKPOINT_NAME = (
    "mysql_sales_binlog_read"
)

APPLY_CHECKPOINT_NAME = (
    "mysql_sales_binlog"
)

SOURCE_SCHEMA = "sales"

DEFAULT_CDC_SERVER_ID = 100


def to_jsonb_or_none(value):
    if value is None:
        return None

    return Jsonb(
        make_json_safe(value)
    )


def insert_raw_events(
    cursor,
    batch_id,
    raw_events,
):
    inserted = 0

    query = """
        INSERT INTO cdc.raw_change_event (
            batch_id,
            event_key,
            operation,
            source_schema,
            source_table,
            before_values,
            after_values,
            binlog_file,
            event_end_position,
            row_index,
            transaction_id,
            commit_position,
            event_timestamp,
            commit_timestamp
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        ON CONFLICT (event_key)
        DO NOTHING
        RETURNING raw_event_id
    """

    for raw_event in raw_events:
        cursor.execute(
            query,
            (
                batch_id,
                raw_event["event_key"],
                raw_event["operation"],
                raw_event["source_schema"],
                raw_event["source_table"],
                to_jsonb_or_none(
                    raw_event[
                        "before_values"
                    ]
                ),
                to_jsonb_or_none(
                    raw_event[
                        "after_values"
                    ]
                ),
                raw_event["binlog_file"],
                raw_event[
                    "event_end_position"
                ],
                raw_event["row_index"],
                raw_event[
                    "transaction_id"
                ],
                raw_event[
                    "commit_position"
                ],
                raw_event[
                    "event_timestamp"
                ],
                raw_event[
                    "commit_timestamp"
                ],
            ),
        )

        if cursor.fetchone() is not None:
            inserted += 1

    return inserted


def persist_raw_transaction(
    batch_id,
    transaction,
    pipeline_name=PIPELINE_NAME,
    checkpoint_name=READ_CHECKPOINT_NAME,
):
    """
    Persist one committed MySQL transaction
    into the RAW CDC layer.

    RAW events and the READ checkpoint are
    committed atomically.
    """

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                rows_inserted = insert_raw_events(
                    cursor=cursor,
                    batch_id=batch_id,
                    raw_events=transaction[
                        "change_events"
                    ],
                )

                upsert_cdc_checkpoint(
                    cursor,
                    pipeline_name,
                    checkpoint_name,
                    transaction["binlog_file"],
                    transaction[
                        "commit_position"
                    ],
                )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

    logger.info(
        "Raw CDC transaction persisted. "
        "batch_id=%s transaction_id=%s "
        "events=%s inserted=%s "
        "read_checkpoint=%s:%s",
        batch_id,
        transaction["transaction_id"],
        len(transaction["change_events"]),
        rows_inserted,
        transaction["binlog_file"],
        transaction["commit_position"],
    )

    return rows_inserted

def extract_cdc_batch(
    batch_id,
    pipeline_name=PIPELINE_NAME,
    read_checkpoint_name=(
        READ_CHECKPOINT_NAME
    ),
    apply_checkpoint_name=(
        APPLY_CHECKPOINT_NAME
    ),
    source_schema=SOURCE_SCHEMA,
):
    """
    Extract committed MySQL CDC transactions
    into the RAW CDC layer.

    The READ checkpoint represents durable
    extraction progress.

    It advances:
    - atomically with persisted RAW transactions;
    - to the safe stream cursor after normal EOF.

    It does not advance after a reader failure.
    """

    connection_settings = (
        get_mysql_cdc_settings()
    )

    checkpoint = get_cdc_checkpoint(
        pipeline_name,
        read_checkpoint_name,
    )

    if checkpoint is None:
        checkpoint = (
            initialize_cdc_checkpoint_from_checkpoint(
                pipeline_name,
                apply_checkpoint_name,
                read_checkpoint_name,
            )
        )

    (
        start_binlog_file,
        start_binlog_position,
    ) = checkpoint

    server_id = int(
        os.environ.get(
            "MYSQL_CDC_SERVER_ID",
            DEFAULT_CDC_SERVER_ID,
        )
    )

    stream = create_cdc_stream(
        connection_settings=connection_settings,
        source_schema=source_schema,
        log_file=start_binlog_file,
        log_position=start_binlog_position,
        server_id=server_id,
    )

    transactions_extracted = 0
    events_extracted = 0
    raw_events_inserted = 0

    end_coordinate = checkpoint
    safe_read_coordinate = None

    try:
        for transaction in (
            iter_raw_committed_transactions(
                stream
            )
        ):
            rows_inserted = (
                persist_raw_transaction(
                    batch_id=batch_id,
                    transaction=transaction,
                    pipeline_name=(
                        pipeline_name
                    ),
                    checkpoint_name=(
                        read_checkpoint_name
                    ),
                )
            )

            transactions_extracted += 1

            events_extracted += len(
                transaction[
                    "change_events"
                ]
            )

            raw_events_inserted += (
                rows_inserted
            )

            end_coordinate = (
                transaction[
                    "binlog_file"
                ],
                transaction[
                    "commit_position"
                ],
            )

        # Only reached after normal
        # iterator exhaustion.
        safe_read_coordinate = (
            stream.log_file,
            stream.log_pos,
        )

    finally:
        stream.close()

    if is_binlog_coordinate_ahead(
        safe_read_coordinate,
        end_coordinate,
    ):
        update_cdc_checkpoint(
            pipeline_name,
            read_checkpoint_name,
            safe_read_coordinate[0],
            safe_read_coordinate[1],
        )

        end_coordinate = (
            safe_read_coordinate
        )

        logger.info(
            "CDC READ checkpoint advanced "
            "to safe stream cursor. "
            "checkpoint=%s:%s",
            end_coordinate[0],
            end_coordinate[1],
        )

    result = {
        "batch_id": batch_id,
        "start_binlog_file": (
            start_binlog_file
        ),
        "start_binlog_position": (
            start_binlog_position
        ),
        "end_binlog_file": (
            end_coordinate[0]
        ),
        "end_binlog_position": (
            end_coordinate[1]
        ),
        "transactions_extracted": (
            transactions_extracted
        ),
        "events_extracted": (
            events_extracted
        ),
        "raw_events_inserted": (
            raw_events_inserted
        ),
    }

    logger.info(
        "CDC extraction completed. "
        "batch_id=%s transactions=%s "
        "events=%s inserted=%s "
        "read_checkpoint=%s:%s",
        batch_id,
        transactions_extracted,
        events_extracted,
        raw_events_inserted,
        end_coordinate[0],
        end_coordinate[1],
    )

    return result
