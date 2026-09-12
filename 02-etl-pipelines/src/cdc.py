import pymysql
from datetime import datetime, timezone
from pymysqlreplication import BinLogStreamReader

from pymysqlreplication import (
    BinLogStreamReader,
)

from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

from pymysqlreplication.event import (
    QueryEvent,
    XidEvent,
)

ROW_EVENT_TYPES = (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)

PRIMARY_KEY_COLUMNS_BY_TABLE_NAME = {
    "categories": ("category_id",),
    "countries": ("country_id",),
    "orders": ("order_id",),
}

CDC_TABLES = tuple(
    PRIMARY_KEY_COLUMNS_BY_TABLE_NAME
)

def get_current_binlog_coordinate(
    connection_settings,
):
    with pymysql.connect(
        **connection_settings
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SHOW MASTER STATUS"
            )

            row = cursor.fetchone()

    if row is None:
        raise RuntimeError(
            "MySQL binary log coordinate "
            "is not available."
        )

    return row[0], row[1]

def binlog_timestamp_to_datetime(timestamp):
    return datetime.fromtimestamp(
        timestamp,
        tz=timezone.utc,
    )

def build_event_key(
    binlog_file,
    event_end_position,
    row_index,
):
    return (
        f"{binlog_file}:"
        f"{event_end_position}:"
        f"{row_index}"
    )


def extract_primary_key(
    values,
    primary_key_columns,
):
    return {
        column: values[column]
        for column in primary_key_columns
    }


def normalize_row_event(
    event,
    binlog_file,
    event_end_position,
    primary_key_columns,
):
    change_events = []

    for row_index, row in enumerate(event.rows):
        if isinstance(event, WriteRowsEvent):
            operation = "INSERT"
            before_values = None
            after_values = row["values"]
            current_values = after_values

        elif isinstance(event, UpdateRowsEvent):
            operation = "UPDATE"
            before_values = row["before_values"]
            after_values = row["after_values"]
            current_values = after_values

        elif isinstance(event, DeleteRowsEvent):
            operation = "DELETE"
            before_values = row["values"]
            after_values = None
            current_values = before_values

        else:
            raise TypeError(
                "Unsupported CDC row event: "
                f"{type(event).__name__}"
            )

        change_event = {
            "event_key": build_event_key(
                binlog_file,
                event_end_position,
                row_index,
            ),
            "operation": operation,
            "source_schema": event.schema,
            "source_table": event.table,
            "primary_key": extract_primary_key(
                current_values,
                primary_key_columns,
            ),
            "before_values": before_values,
            "after_values": after_values,
            "binlog_file": binlog_file,
            "event_end_position": event_end_position,
            "row_index": row_index,
            "event_timestamp": (
                binlog_timestamp_to_datetime(
                    event.timestamp
                )
            ),
        }

        change_events.append(change_event)

    return change_events

def iter_raw_committed_transactions(
    stream,
):
    transaction_buffer = None

    for event in stream:
        if isinstance(event, QueryEvent):
            query = event.query

            if isinstance(query, bytes):
                query = query.decode("utf-8")

            if query.strip().upper() == "BEGIN":
                transaction_buffer = (
                    start_transaction_buffer()
                )

            continue

        if isinstance(event, ROW_EVENT_TYPES):
            if transaction_buffer is None:
                raise RuntimeError(
                    "CDC row event received outside "
                    "an active transaction."
                )

            raw_events = (
                normalize_raw_row_event(
                    event=event,
                    binlog_file=stream.log_file,
                    event_end_position=(
                        stream.log_pos
                    ),
                )
            )

            append_change_events(
                transaction_buffer,
                raw_events,
            )

            continue

        if isinstance(event, XidEvent):
            if transaction_buffer is None:
                continue

            if not transaction_buffer[
                "change_events"
            ]:
                transaction_buffer = None
                continue

            transaction = (
                commit_transaction_buffer(
                    transaction_buffer,
                    transaction_id=event.xid,
                    binlog_file=(
                        stream.log_file
                    ),
                    commit_position=(
                        stream.log_pos
                    ),
                    commit_timestamp=(
                        binlog_timestamp_to_datetime(
                            event.timestamp
                        )
                    ),
                )
            )

            transaction_buffer = None

            yield transaction

def normalize_raw_row_event(
    event,
    binlog_file,
    event_end_position,
):
    raw_events = []

    if isinstance(event, WriteRowsEvent):
        operation = "INSERT"

    elif isinstance(event, UpdateRowsEvent):
        operation = "UPDATE"

    elif isinstance(event, DeleteRowsEvent):
        operation = "DELETE"

    else:
        raise TypeError(
            "Unsupported CDC row event: "
            f"{type(event).__name__}"
        )

    for row_index, row in enumerate(
        event.rows
    ):
        if operation == "INSERT":
            before_values = None
            after_values = row["values"]

        elif operation == "UPDATE":
            before_values = row[
                "before_values"
            ]
            after_values = row[
                "after_values"
            ]

        else:
            before_values = row["values"]
            after_values = None

        raw_events.append(
            {
                "event_key": build_event_key(
                    binlog_file,
                    event_end_position,
                    row_index,
                ),
                "operation": operation,
                "source_schema": event.schema,
                "source_table": event.table,
                "before_values": before_values,
                "after_values": after_values,
                "binlog_file": binlog_file,
                "event_end_position": (
                    event_end_position
                ),
                "row_index": row_index,
                "event_timestamp": (
                    binlog_timestamp_to_datetime(
                        event.timestamp
                    )
                ),
            }
        )

    return raw_events


def start_transaction_buffer():
    return {
        "change_events": [],
    }


def append_change_events(
    transaction_buffer,
    change_events,
):
    transaction_buffer["change_events"].extend(
        change_events
    )


def commit_transaction_buffer(
    transaction_buffer,
    transaction_id,
    binlog_file,
    commit_position,
    commit_timestamp,
):
    committed_events = []

    for change_event in transaction_buffer[
        "change_events"
    ]:
        committed_event = dict(change_event)

        committed_event["transaction_id"] = (
            transaction_id
        )
        committed_event["commit_position"] = (
            commit_position
        )
        committed_event["commit_timestamp"] = (
            commit_timestamp
        )

        committed_events.append(
            committed_event
        )

    return {
        "transaction_id": transaction_id,
        "binlog_file": binlog_file,
        "commit_position": commit_position,
        "commit_timestamp": commit_timestamp,
        "change_events": committed_events,
    }


def iter_committed_transactions(
    stream,
    primary_key_columns_by_table,
):
    transaction_buffer = None

    for event in stream:
        if isinstance(event, QueryEvent):
            query = event.query

            if isinstance(query, bytes):
                query = query.decode("utf-8")

            if query.strip().upper() == "BEGIN":
                transaction_buffer = (
                    start_transaction_buffer()
                )

            continue

        if isinstance(event, ROW_EVENT_TYPES):
            if transaction_buffer is None:
                raise RuntimeError(
                    "CDC row event received outside "
                    "an active transaction."
                )

            table_key = (
                event.schema,
                event.table,
            )

            if (
                table_key
                not in primary_key_columns_by_table
            ):
                raise KeyError(
                    "Primary key metadata not configured "
                    f"for {event.schema}.{event.table}."
                )

            change_events = normalize_row_event(
                event=event,
                binlog_file=stream.log_file,
                event_end_position=stream.log_pos,
                primary_key_columns=(
                    primary_key_columns_by_table[
                        table_key
                    ]
                ),
            )

            append_change_events(
                transaction_buffer,
                change_events,
            )

            continue

        if isinstance(event, XidEvent):
            if transaction_buffer is None:
                continue

            if not transaction_buffer[
                "change_events"
            ]:
                transaction_buffer = None
                continue

            transaction = commit_transaction_buffer(
                transaction_buffer,
                transaction_id=event.xid,
                binlog_file=stream.log_file,
                commit_position=stream.log_pos,
                commit_timestamp=binlog_timestamp_to_datetime(
                    event.timestamp
                ),
            )

            transaction_buffer = None

            yield transaction

def build_primary_key_columns_by_table(
    source_schema,
):
    return {
        (
            source_schema,
            table_name,
        ): primary_key_columns
        for (
            table_name,
            primary_key_columns,
        ) in (
            PRIMARY_KEY_COLUMNS_BY_TABLE_NAME.items()
        )
    }


def create_cdc_stream(
    connection_settings,
    source_schema,
    log_file,
    log_position,
    server_id,
):
    return BinLogStreamReader(
        connection_settings=connection_settings,
        server_id=server_id,
        blocking=False,
        resume_stream=True,
        log_file=log_file,
        log_pos=log_position,
        only_schemas=[source_schema],
        only_tables=list(CDC_TABLES),
        only_events=[
            QueryEvent,
            WriteRowsEvent,
            UpdateRowsEvent,
            DeleteRowsEvent,
            XidEvent,
        ],
    )

def binlog_file_sequence(binlog_file):
    try:
        return int(
            binlog_file.rsplit(".", 1)[1]
        )
    except (
        IndexError,
        ValueError,
    ) as exc:
        raise ValueError(
            "Unsupported binlog file name: "
            f"{binlog_file!r}"
        ) from exc

def is_binlog_coordinate_ahead(
    candidate,
    current,
):
    candidate_file, candidate_position = (
        candidate
    )

    current_file, current_position = (
        current
    )

    if candidate_file == current_file:
        return (
            candidate_position
            > current_position
        )

    return (
        binlog_file_sequence(
            candidate_file
        )
        > binlog_file_sequence(
            current_file
        )
    )