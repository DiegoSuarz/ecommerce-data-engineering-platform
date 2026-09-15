from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from pymysqlreplication.event import (
    QueryEvent,
    XidEvent,
)

from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

from cdc.stream import (
    append_change_events,
    binlog_timestamp_to_datetime,
    build_event_key,
    build_primary_key_columns_by_table,
    commit_transaction_buffer,
    extract_primary_key,
    iter_raw_committed_transactions,
    normalize_raw_row_event,
    start_transaction_buffer,
)


class FakeBinLogStream:
    def __init__(
        self,
        binlog_file,
        events,
    ):
        self.log_file = binlog_file
        self.events = events
        self.log_pos = None

    def __iter__(self):
        for event, log_pos in self.events:
            self.log_pos = log_pos
            yield event


def build_mock_event(
    event_class,
    rows,
    timestamp=1234567890,
):
    event = Mock(spec=event_class)

    event.schema = "sales"
    event.table = "categories"
    event.rows = rows
    event.timestamp = timestamp

    return event


def test_build_event_key():
    event_key = build_event_key(
        "binlog.000028",
        4151,
        0,
    )

    assert event_key == "binlog.000028:4151:0"


def test_extract_primary_key():
    values = {
        "category_id": 900004,
        "category_name": "CDC Test",
    }

    primary_key = extract_primary_key(
        values,
        ("category_id",),
    )

    assert primary_key == {
        "category_id": 900004,
    }


def test_extract_composite_primary_key():
    values = {
        "order_id": 100,
        "line_id": 2,
        "amount": 50,
    }

    primary_key = extract_primary_key(
        values,
        (
            "order_id",
            "line_id",
        ),
    )

    assert primary_key == {
        "order_id": 100,
        "line_id": 2,
    }











def test_start_transaction_buffer():
    transaction_buffer = (
        start_transaction_buffer()
    )

    assert transaction_buffer == {
        "change_events": [],
    }


def test_append_change_events():
    transaction_buffer = (
        start_transaction_buffer()
    )

    change_events = [
        {
            "event_key": "binlog.000028:100:0",
        },
        {
            "event_key": "binlog.000028:100:1",
        },
    ]

    append_change_events(
        transaction_buffer,
        change_events,
    )

    assert transaction_buffer[
        "change_events"
    ] == change_events


def test_commit_transaction_buffer():
    transaction_buffer = (
        start_transaction_buffer()
    )

    append_change_events(
        transaction_buffer,
        [
            {
                "event_key":
                    "binlog.000028:3284:0",
                "operation": "UPDATE",
            },
            {
                "event_key":
                    "binlog.000028:3284:1",
                "operation": "UPDATE",
            },
        ],
    )

    commit_timestamp = datetime(
        2026,
        9,
        9,
        2,
        0,
        tzinfo=timezone.utc,
    )

    transaction = (
    commit_transaction_buffer(
        transaction_buffer,
        transaction_id=2095,
        binlog_file="binlog.000028",
        commit_position=3315,
        commit_timestamp=commit_timestamp,
        )
    )

    assert transaction["transaction_id"] == 2095
    assert transaction["binlog_file"] == (
        "binlog.000028"
    )
    assert transaction["commit_position"] == 3315
    assert (
        transaction["commit_timestamp"]
        == commit_timestamp
    )

    assert len(
        transaction["change_events"]
    ) == 2
    for change_event in transaction[
        "change_events"
    ]:
        assert (
            change_event["transaction_id"]
            == 2095
        )
        assert (
            change_event["commit_position"]
            == 3315
        )
        assert (
        transaction["commit_timestamp"]
        == commit_timestamp
        )






def test_binlog_timestamp_to_datetime():
    result = binlog_timestamp_to_datetime(
        1234567890
    )

    assert result == datetime(
        2009,
        2,
        13,
        23,
        31,
        30,
        tzinfo=timezone.utc,
    )

def test_build_primary_key_columns_by_table():
    result = build_primary_key_columns_by_table(
        "sales"
    )

    assert result == {
        (
            "sales",
            "categories",
        ): ("category_id",),
        (
            "sales",
            "countries",
        ): ("country_id",),
        (
            "sales",
            "orders",
        ): ("order_id",),
    }

def test_normalize_raw_insert_event():
    values = {
        "category_id": 900030,
        "category_name": "Raw Insert",
    }

    event = build_mock_event(
        WriteRowsEvent,
        [
            {
                "values": values,
            }
        ],
    )

    raw_events = normalize_raw_row_event(
        event=event,
        binlog_file="binlog.000040",
        event_end_position=500,
    )

    assert len(raw_events) == 1

    raw_event = raw_events[0]

    assert (
        raw_event["event_key"]
        == "binlog.000040:500:0"
    )

    assert raw_event["operation"] == "INSERT"

    assert (
        raw_event["source_schema"]
        == "sales"
    )

    assert (
        raw_event["source_table"]
        == "categories"
    )

    assert raw_event["before_values"] is None
    assert raw_event["after_values"] == values

    assert "primary_key" not in raw_event


def test_normalize_raw_update_event():
    before_values = {
        "category_id": 900030,
        "category_name": "Before",
    }

    after_values = {
        "category_id": 900030,
        "category_name": "After",
    }

    event = build_mock_event(
        UpdateRowsEvent,
        [
            {
                "before_values": before_values,
                "after_values": after_values,
            }
        ],
    )

    raw_events = normalize_raw_row_event(
        event=event,
        binlog_file="binlog.000040",
        event_end_position=700,
    )

    raw_event = raw_events[0]

    assert raw_event["operation"] == "UPDATE"

    assert (
        raw_event["before_values"]
        == before_values
    )

    assert (
        raw_event["after_values"]
        == after_values
    )

    assert "primary_key" not in raw_event


def test_normalize_raw_delete_event():
    values = {
        "category_id": 900030,
        "category_name": "Deleted",
    }

    event = build_mock_event(
        DeleteRowsEvent,
        [
            {
                "values": values,
            }
        ],
    )

    raw_events = normalize_raw_row_event(
        event=event,
        binlog_file="binlog.000040",
        event_end_position=900,
    )

    raw_event = raw_events[0]

    assert raw_event["operation"] == "DELETE"

    assert raw_event["before_values"] == values
    assert raw_event["after_values"] is None

    assert "primary_key" not in raw_event


def test_normalize_raw_multiple_rows():
    event = build_mock_event(
        WriteRowsEvent,
        [
            {
                "values": {
                    "category_id": 900031,
                },
            },
            {
                "values": {
                    "category_id": 900032,
                },
            },
        ],
    )

    raw_events = normalize_raw_row_event(
        event=event,
        binlog_file="binlog.000040",
        event_end_position=1000,
    )

    assert len(raw_events) == 2

    assert (
        raw_events[0]["event_key"]
        == "binlog.000040:1000:0"
    )

    assert (
        raw_events[1]["event_key"]
        == "binlog.000040:1000:1"
    )

    assert "primary_key" not in raw_events[0]
    assert "primary_key" not in raw_events[1]

def test_iter_raw_committed_transactions():
    begin_event = Mock(
        spec=QueryEvent
    )
    begin_event.query = "BEGIN"

    row_event = build_mock_event(
        WriteRowsEvent,
        [
            {
                "values": {
                    "category_id": 900033,
                    "category_name": (
                        "Raw Transaction"
                    ),
                },
            }
        ],
        timestamp=1234567890,
    )

    xid_event = Mock(
        spec=XidEvent
    )
    xid_event.xid = 777
    xid_event.timestamp = 1234567891

    stream = FakeBinLogStream(
        binlog_file="binlog.000040",
        events=[
            (
                begin_event,
                300,
            ),
            (
                row_event,
                600,
            ),
            (
                xid_event,
                650,
            ),
        ],
    )

    transactions = list(
        iter_raw_committed_transactions(
            stream
        )
    )

    assert len(transactions) == 1

    transaction = transactions[0]

    assert (
        transaction["transaction_id"]
        == 777
    )

    assert (
        transaction["binlog_file"]
        == "binlog.000040"
    )

    assert (
        transaction["commit_position"]
        == 650
    )

    assert len(
        transaction["change_events"]
    ) == 1

    raw_event = transaction[
        "change_events"
    ][0]

    assert (
        raw_event["event_key"]
        == "binlog.000040:600:0"
    )

    assert (
        raw_event["operation"]
        == "INSERT"
    )

    assert (
        raw_event["transaction_id"]
        == 777
    )

    assert (
        raw_event["commit_position"]
        == 650
    )

    assert "primary_key" not in raw_event


def test_get_current_binlog_coordinate(
    monkeypatch,
):
    from unittest.mock import MagicMock

    from cdc import stream as cdc_stream

    connection = MagicMock()
    cursor = MagicMock()

    connection.cursor.return_value = cursor

    cursor.fetchone.return_value = (
        "binlog.000042",
        157,
        "",
        "",
        "",
    )

    monkeypatch.setattr(
        cdc_stream,
        "get_mysql_cdc_connection",
        lambda: connection,
    )

    coordinate = (
        cdc_stream
        .get_current_binlog_coordinate()
    )

    assert coordinate == (
        "binlog.000042",
        157,
    )

    cursor.execute.assert_called_once_with(
        "SHOW MASTER STATUS"
    )

    cursor.close.assert_called_once_with()
    connection.close.assert_called_once_with()


def test_get_current_binlog_coordinate_requires_binary_log(
    monkeypatch,
):
    from unittest.mock import MagicMock

    from cdc import stream as cdc_stream

    connection = MagicMock()
    cursor = MagicMock()

    connection.cursor.return_value = cursor
    cursor.fetchone.return_value = None

    monkeypatch.setattr(
        cdc_stream,
        "get_mysql_cdc_connection",
        lambda: connection,
    )

    with pytest.raises(
        RuntimeError,
        match="binary log status",
    ):
        (
            cdc_stream
            .get_current_binlog_coordinate()
        )

    cursor.close.assert_called_once_with()
    connection.close.assert_called_once_with()
