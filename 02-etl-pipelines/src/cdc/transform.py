from psycopg.types.json import Jsonb

from cdc.stream import (
    build_primary_key_columns_by_table,
    extract_primary_key,
)
from db import get_postgres_connection
from load import make_json_safe
from logger import get_logger


logger = get_logger("cdc.transform")


SOURCE_SCHEMA = "sales"


RAW_EVENT_COLUMNS = (
    "raw_event_id",
    "batch_id",
    "event_key",
    "operation",
    "source_schema",
    "source_table",
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


def to_jsonb_or_none(value):
    if value is None:
        return None

    return Jsonb(
        make_json_safe(value)
    )


def validate_raw_event_semantics(
    raw_event,
):
    operation = raw_event["operation"]
    before_values = raw_event[
        "before_values"
    ]
    after_values = raw_event[
        "after_values"
    ]

    if operation == "INSERT":
        valid = (
            before_values is None
            and after_values is not None
        )

    elif operation == "UPDATE":
        valid = (
            before_values is not None
            and after_values is not None
        )

    elif operation == "DELETE":
        valid = (
            before_values is not None
            and after_values is None
        )

    else:
        raise ValueError(
            "Unsupported CDC operation: "
            f"{operation!r}"
        )

    if not valid:
        raise ValueError(
            "Invalid CDC before/after "
            "semantics for operation "
            f"{operation!r}."
        )


def transform_raw_event(
    raw_event,
    primary_key_columns_by_table,
):
    validate_raw_event_semantics(
        raw_event
    )

    table_key = (
        raw_event["source_schema"],
        raw_event["source_table"],
    )

    if (
        table_key
        not in primary_key_columns_by_table
    ):
        raise KeyError(
            "Primary key metadata not "
            "configured for "
            f"{table_key[0]}.{table_key[1]}."
        )

    if raw_event["operation"] == "DELETE":
        primary_key_source = raw_event[
            "before_values"
        ]
    else:
        primary_key_source = raw_event[
            "after_values"
        ]

    primary_key = extract_primary_key(
        primary_key_source,
        primary_key_columns_by_table[
            table_key
        ],
    )

    return {
        **raw_event,
        "primary_key": primary_key,
    }


def fetch_raw_events(
    cursor,
    batch_id,
):
    cursor.execute(
        """
        SELECT
            raw_event_id,
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
        FROM cdc.raw_change_event
        WHERE batch_id = %s
        ORDER BY raw_event_id
        """,
        (batch_id,),
    )

    return [
        dict(
            zip(
                RAW_EVENT_COLUMNS,
                row,
            )
        )
        for row in cursor.fetchall()
    ]


def insert_transformed_events(
    cursor,
    transformed_events,
):
    inserted = 0

    query = """
        INSERT INTO cdc.transformed_event (
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
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (event_key)
        DO NOTHING
        RETURNING transformed_event_id
    """

    for event in transformed_events:
        cursor.execute(
            query,
            (
                event["batch_id"],
                event["raw_event_id"],
                event["event_key"],
                event["operation"],
                event["source_schema"],
                event["source_table"],
                Jsonb(
                    make_json_safe(
                        event["primary_key"]
                    )
                ),
                to_jsonb_or_none(
                    event["before_values"]
                ),
                to_jsonb_or_none(
                    event["after_values"]
                ),
                event["binlog_file"],
                event[
                    "event_end_position"
                ],
                event["row_index"],
                event["transaction_id"],
                event[
                    "commit_position"
                ],
                event["event_timestamp"],
                event["commit_timestamp"],
            ),
        )

        if cursor.fetchone() is not None:
            inserted += 1

    return inserted


def transform_cdc_batch(
    batch_id,
    source_schema=SOURCE_SCHEMA,
):
    primary_key_columns_by_table = (
        build_primary_key_columns_by_table(
            source_schema
        )
    )

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                raw_events = fetch_raw_events(
                    cursor=cursor,
                    batch_id=batch_id,
                )

                transformed_events = [
                    transform_raw_event(
                        raw_event,
                        primary_key_columns_by_table,
                    )
                    for raw_event in raw_events
                ]

                rows_inserted = (
                    insert_transformed_events(
                        cursor=cursor,
                        transformed_events=(
                            transformed_events
                        ),
                    )
                )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

    result = {
        "batch_id": batch_id,
        "raw_events_read": len(
            raw_events
        ),
        "events_transformed": len(
            transformed_events
        ),
        "transformed_events_inserted": (
            rows_inserted
        ),
    }

    logger.info(
        "CDC transformation completed. "
        "batch_id=%s raw=%s "
        "transformed=%s inserted=%s",
        batch_id,
        len(raw_events),
        len(transformed_events),
        rows_inserted,
    )

    return result
