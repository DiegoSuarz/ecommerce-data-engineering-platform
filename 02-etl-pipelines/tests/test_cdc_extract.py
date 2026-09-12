from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

import cdc_extract


def build_transaction():
    timestamp = datetime(
        2026,
        9,
        11,
        22,
        0,
        tzinfo=timezone.utc,
    )

    return {
        "transaction_id": 9001,
        "binlog_file": "binlog.000040",
        "commit_position": 1200,
        "commit_timestamp": timestamp,
        "change_events": [
            {
                "event_key":
                    "binlog.000040:1100:0",
                "operation": "INSERT",
                "source_schema": "sales",
                "source_table": "categories",
                "before_values": None,
                "after_values": {
                    "category_id": 900040,
                    "category_name": "Raw Test",
                },
                "binlog_file":
                    "binlog.000040",
                "event_end_position": 1100,
                "row_index": 0,
                "transaction_id": 9001,
                "commit_position": 1200,
                "event_timestamp": timestamp,
                "commit_timestamp": timestamp,
            }
        ],
    }


def build_connection():
    connection = MagicMock()
    cursor = MagicMock()

    connection.__enter__.return_value = (
        connection
    )

    connection.cursor.return_value\
        .__enter__.return_value = cursor

    return connection, cursor


def test_insert_raw_events_is_idempotent():
    cursor = MagicMock()

    cursor.fetchone.side_effect = [
        (101,),
        None,
    ]

    transaction = build_transaction()

    raw_events = [
        transaction["change_events"][0],
        {
            **transaction[
                "change_events"
            ][0],
            "event_key":
                "binlog.000040:1100:1",
            "row_index": 1,
        },
    ]

    inserted = cdc_extract.insert_raw_events(
        cursor=cursor,
        batch_id=77,
        raw_events=raw_events,
    )

    assert inserted == 1
    assert cursor.execute.call_count == 2

    query = cursor.execute.call_args_list[
        0
    ].args[0]

    assert (
        "ON CONFLICT (event_key)"
        in query
    )

    assert "primary_key" not in query


def test_persist_raw_transaction_is_atomic(
    monkeypatch,
):
    connection, cursor = (
        build_connection()
    )

    transaction = build_transaction()

    insert_calls = []
    checkpoint_calls = []

    def insert_raw(
        cursor,
        batch_id,
        raw_events,
    ):
        insert_calls.append(
            (
                cursor,
                batch_id,
                raw_events,
            )
        )

        return 1

    def upsert_checkpoint(
        cursor,
        pipeline_name,
        checkpoint_name,
        binlog_file,
        binlog_position,
    ):
        checkpoint_calls.append(
            (
                cursor,
                pipeline_name,
                checkpoint_name,
                binlog_file,
                binlog_position,
            )
        )

    monkeypatch.setattr(
        cdc_extract,
        "get_postgres_connection",
        lambda: connection,
    )

    monkeypatch.setattr(
        cdc_extract,
        "insert_raw_events",
        insert_raw,
    )

    monkeypatch.setattr(
        cdc_extract,
        "upsert_cdc_checkpoint",
        upsert_checkpoint,
    )

    inserted = (
        cdc_extract.persist_raw_transaction(
            batch_id=77,
            transaction=transaction,
        )
    )

    assert inserted == 1

    assert insert_calls == [
        (
            cursor,
            77,
            transaction[
                "change_events"
            ],
        )
    ]

    assert checkpoint_calls == [
        (
            cursor,
            cdc_extract.PIPELINE_NAME,
            cdc_extract.READ_CHECKPOINT_NAME,
            "binlog.000040",
            1200,
        )
    ]

    connection.commit.assert_called_once_with()
    connection.rollback.assert_not_called()


def test_persist_raw_transaction_rolls_back_when_checkpoint_fails(
    monkeypatch,
):
    connection, _ = build_connection()

    transaction = build_transaction()

    monkeypatch.setattr(
        cdc_extract,
        "get_postgres_connection",
        lambda: connection,
    )

    monkeypatch.setattr(
        cdc_extract,
        "insert_raw_events",
        lambda **kwargs: 1,
    )

    def fail_checkpoint(*args, **kwargs):
        raise RuntimeError(
            "checkpoint failure"
        )

    monkeypatch.setattr(
        cdc_extract,
        "upsert_cdc_checkpoint",
        fail_checkpoint,
    )

    with pytest.raises(
        RuntimeError,
        match="checkpoint failure",
    ):
        cdc_extract.persist_raw_transaction(
            batch_id=77,
            transaction=transaction,
        )

    connection.commit.assert_not_called()
    connection.rollback.assert_called_once_with()


def test_duplicate_raw_replay_still_advances_checkpoint(
    monkeypatch,
):
    connection, _ = build_connection()

    transaction = build_transaction()

    checkpoint_calls = []

    monkeypatch.setattr(
        cdc_extract,
        "get_postgres_connection",
        lambda: connection,
    )

    monkeypatch.setattr(
        cdc_extract,
        "insert_raw_events",
        lambda **kwargs: 0,
    )

    def upsert_checkpoint(
        cursor,
        pipeline_name,
        checkpoint_name,
        binlog_file,
        binlog_position,
    ):
        checkpoint_calls.append(
            (
                pipeline_name,
                checkpoint_name,
                binlog_file,
                binlog_position,
            )
        )

    monkeypatch.setattr(
        cdc_extract,
        "upsert_cdc_checkpoint",
        upsert_checkpoint,
    )

    inserted = (
        cdc_extract.persist_raw_transaction(
            batch_id=77,
            transaction=transaction,
        )
    )

    assert inserted == 0

    assert checkpoint_calls == [
        (
            cdc_extract.PIPELINE_NAME,
            cdc_extract.READ_CHECKPOINT_NAME,
            "binlog.000040",
            1200,
        )
    ]

    connection.commit.assert_called_once_with()
    connection.rollback.assert_not_called()

def test_binlog_coordinate_ahead_same_file():
    assert (
        cdc_extract
        .is_binlog_coordinate_ahead(
            (
                "binlog.000040",
                900,
            ),
            (
                "binlog.000040",
                700,
            ),
        )
        is True
    )


def test_binlog_coordinate_behind_same_file():
    assert (
        cdc_extract
        .is_binlog_coordinate_ahead(
            (
                "binlog.000040",
                500,
            ),
            (
                "binlog.000040",
                700,
            ),
        )
        is False
    )


def test_binlog_coordinate_ahead_after_rotation():
    assert (
        cdc_extract
        .is_binlog_coordinate_ahead(
            (
                "binlog.000041",
                157,
            ),
            (
                "binlog.000040",
                9000,
            ),
        )
        is True
    )

def test_extract_cdc_batch_advances_safe_eof(
    monkeypatch,
):
    stream = MagicMock()

    stream.log_file = "binlog.000041"
    stream.log_pos = 157

    checkpoint_updates = []

    monkeypatch.setattr(
        cdc_extract,
        "get_mysql_cdc_settings",
        lambda: {
            "host": "mysql",
        },
    )

    monkeypatch.setattr(
        cdc_extract,
        "get_cdc_checkpoint",
        lambda *args, **kwargs: (
            "binlog.000040",
            9000,
        ),
    )

    monkeypatch.setattr(
        cdc_extract,
        "create_cdc_stream",
        lambda **kwargs: stream,
    )

    monkeypatch.setattr(
        cdc_extract,
        "iter_raw_committed_transactions",
        lambda stream: iter([]),
    )

    monkeypatch.setattr(
        cdc_extract,
        "update_cdc_checkpoint",
        lambda *args: (
            checkpoint_updates.append(
                args
            )
        ),
    )

    result = (
        cdc_extract.extract_cdc_batch(
            batch_id=88
        )
    )

    assert result[
        "transactions_extracted"
    ] == 0

    assert result[
        "events_extracted"
    ] == 0

    assert result[
        "raw_events_inserted"
    ] == 0

    assert result[
        "start_binlog_file"
    ] == "binlog.000040"

    assert result[
        "end_binlog_file"
    ] == "binlog.000041"

    assert result[
        "end_binlog_position"
    ] == 157

    assert checkpoint_updates == [
        (
            cdc_extract.PIPELINE_NAME,
            cdc_extract.READ_CHECKPOINT_NAME,
            "binlog.000041",
            157,
        )
    ]

    stream.close.assert_called_once_with()


def test_extract_cdc_batch_persists_transaction(
    monkeypatch,
):
    stream = MagicMock()

    stream.log_file = "binlog.000040"
    stream.log_pos = 1200

    transaction = build_transaction()

    persisted = []

    monkeypatch.setattr(
        cdc_extract,
        "get_mysql_cdc_settings",
        lambda: {
            "host": "mysql",
        },
    )

    monkeypatch.setattr(
        cdc_extract,
        "get_cdc_checkpoint",
        lambda *args, **kwargs: (
            "binlog.000040",
            1000,
        ),
    )

    monkeypatch.setattr(
        cdc_extract,
        "create_cdc_stream",
        lambda **kwargs: stream,
    )

    monkeypatch.setattr(
        cdc_extract,
        "iter_raw_committed_transactions",
        lambda stream: iter(
            [transaction]
        ),
    )

    def persist(**kwargs):
        persisted.append(kwargs)

        return 1

    monkeypatch.setattr(
        cdc_extract,
        "persist_raw_transaction",
        persist,
    )

    update_checkpoint = MagicMock()

    monkeypatch.setattr(
        cdc_extract,
        "update_cdc_checkpoint",
        update_checkpoint,
    )

    result = (
        cdc_extract.extract_cdc_batch(
            batch_id=88
        )
    )

    assert result[
        "transactions_extracted"
    ] == 1

    assert result[
        "events_extracted"
    ] == 1

    assert result[
        "raw_events_inserted"
    ] == 1

    assert result[
        "end_binlog_file"
    ] == "binlog.000040"

    assert result[
        "end_binlog_position"
    ] == 1200

    assert persisted[0][
        "batch_id"
    ] == 88

    assert persisted[0][
        "transaction"
    ] == transaction

    # Transaction persistence already
    # advances READ atomically.
    update_checkpoint.assert_not_called()

    stream.close.assert_called_once_with()

def test_extract_cdc_batch_bootstraps_read_checkpoint(
    monkeypatch,
):
    stream = MagicMock()

    stream.log_file = "binlog.000040"
    stream.log_pos = 1000

    bootstrap_calls = []

    monkeypatch.setattr(
        cdc_extract,
        "get_mysql_cdc_settings",
        lambda: {
            "host": "mysql",
        },
    )

    monkeypatch.setattr(
        cdc_extract,
        "get_cdc_checkpoint",
        lambda *args, **kwargs: None,
    )

    def bootstrap(
        pipeline_name,
        source_checkpoint_name,
        target_checkpoint_name,
    ):
        bootstrap_calls.append(
            (
                pipeline_name,
                source_checkpoint_name,
                target_checkpoint_name,
            )
        )

        return (
            "binlog.000040",
            1000,
        )

    monkeypatch.setattr(
        cdc_extract,
        (
            "initialize_cdc_checkpoint_"
            "from_checkpoint"
        ),
        bootstrap,
    )

    monkeypatch.setattr(
        cdc_extract,
        "create_cdc_stream",
        lambda **kwargs: stream,
    )

    monkeypatch.setattr(
        cdc_extract,
        "iter_raw_committed_transactions",
        lambda stream: iter([]),
    )

    update_checkpoint = MagicMock()

    monkeypatch.setattr(
        cdc_extract,
        "update_cdc_checkpoint",
        update_checkpoint,
    )

    result = (
        cdc_extract.extract_cdc_batch(
            batch_id=88
        )
    )

    assert bootstrap_calls == [
        (
            cdc_extract.PIPELINE_NAME,
            cdc_extract.APPLY_CHECKPOINT_NAME,
            cdc_extract.READ_CHECKPOINT_NAME,
        )
    ]

    assert result[
        "start_binlog_position"
    ] == 1000

    assert result[
        "end_binlog_position"
    ] == 1000

    update_checkpoint.assert_not_called()

    stream.close.assert_called_once_with()

def test_extract_cdc_batch_does_not_advance_safe_cursor_on_failure(
    monkeypatch,
):
    stream = MagicMock()

    stream.log_file = "binlog.000041"
    stream.log_pos = 157

    monkeypatch.setattr(
        cdc_extract,
        "get_mysql_cdc_settings",
        lambda: {
            "host": "mysql",
        },
    )

    monkeypatch.setattr(
        cdc_extract,
        "get_cdc_checkpoint",
        lambda *args, **kwargs: (
            "binlog.000040",
            9000,
        ),
    )

    monkeypatch.setattr(
        cdc_extract,
        "create_cdc_stream",
        lambda **kwargs: stream,
    )

    def fail_reader(stream):
        raise RuntimeError(
            "binlog reader failure"
        )

        yield

    monkeypatch.setattr(
        cdc_extract,
        "iter_raw_committed_transactions",
        fail_reader,
    )

    update_checkpoint = MagicMock()

    monkeypatch.setattr(
        cdc_extract,
        "update_cdc_checkpoint",
        update_checkpoint,
    )

    with pytest.raises(
        RuntimeError,
        match="binlog reader failure",
    ):
        cdc_extract.extract_cdc_batch(
            batch_id=88
        )

    update_checkpoint.assert_not_called()

    stream.close.assert_called_once_with()
