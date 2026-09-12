from datetime import date, datetime, timezone
from decimal import Decimal

from unittest.mock import MagicMock

import pytest

import cdc_load

from audit import (
    start_cdc_batch,
    start_etl_run,
)
from db import get_postgres_connection
from load import (
    load_change_events,
    make_json_safe,
)

from audit import (
    get_cdc_checkpoint,
    initialize_cdc_checkpoint,
    start_cdc_batch,
    start_etl_run,
)
from cdc_load import persist_cdc_transaction
from db import get_postgres_connection


TEST_PIPELINE_NAME = (
    "test_cdc_atomic_transaction"
)

TEST_CHECKPOINT_NAME = (
    "test_mysql_binlog"
)


TEST_PIPELINE_NAME = "test_cdc_change_event_load"


@pytest.fixture
def clean_test_change_events():
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM cdc.change_event
                WHERE batch_id IN (
                    SELECT batch_id
                    FROM audit.cdc_batch
                    WHERE run_id IN (
                        SELECT run_id
                        FROM audit.etl_run
                        WHERE pipeline_name = %s
                    )
                );
                """,
                (TEST_PIPELINE_NAME,),
            )

            cursor.execute(
                """
                DELETE FROM audit.cdc_batch
                WHERE run_id IN (
                    SELECT run_id
                    FROM audit.etl_run
                    WHERE pipeline_name = %s
                );
                """,
                (TEST_PIPELINE_NAME,),
            )

            cursor.execute(
                """
                DELETE FROM audit.etl_run
                WHERE pipeline_name = %s;
                """,
                (TEST_PIPELINE_NAME,),
            )

        connection.commit()

    yield

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM cdc.change_event
                WHERE batch_id IN (
                    SELECT batch_id
                    FROM audit.cdc_batch
                    WHERE run_id IN (
                        SELECT run_id
                        FROM audit.etl_run
                        WHERE pipeline_name = %s
                    )
                );
                """,
                (TEST_PIPELINE_NAME,),
            )

            cursor.execute(
                """
                DELETE FROM audit.cdc_batch
                WHERE run_id IN (
                    SELECT run_id
                    FROM audit.etl_run
                    WHERE pipeline_name = %s
                );
                """,
                (TEST_PIPELINE_NAME,),
            )

            cursor.execute(
                """
                DELETE FROM audit.etl_run
                WHERE pipeline_name = %s;
                """,
                (TEST_PIPELINE_NAME,),
            )

        connection.commit()


def create_test_batch():
    run_id = start_etl_run(
        TEST_PIPELINE_NAME
    )

    return start_cdc_batch(
        run_id=run_id,
        start_binlog_file="binlog.000029",
        start_binlog_position=8697,
    )


def build_change_event():
    return {
        "event_key": "binlog.000029:9000:0",
        "operation": "INSERT",
        "source_schema": "sales",
        "source_table": "orders",
        "primary_key": {
            "order_id": 900001,
        },
        "before_values": None,
        "after_values": {
            "order_id": 900001,
            "order_date": date(
                2026,
                9,
                9,
            ),
            "amount": Decimal("123.45"),
            "updated_at": datetime(
                2026,
                9,
                9,
                18,
                0,
                0,
            ),
        },
        "binlog_file": "binlog.000029",
        "event_end_position": 9000,
        "row_index": 0,
        "transaction_id": 4000,
        "commit_position": 9031,
        "event_timestamp": datetime(
            2026,
            9,
            9,
            18,
            0,
            0,
            tzinfo=timezone.utc,
        ),
        "commit_timestamp": datetime(
            2026,
            9,
            9,
            18,
            0,
            1,
            tzinfo=timezone.utc,
        ),
    }

def test_make_json_safe():
    value = {
        "datetime": datetime(
            2026,
            9,
            9,
            18,
            0,
        ),
        "date": date(
            2026,
            9,
            9,
        ),
        "decimal": Decimal("123.45"),
        "bytes": b"\x01\x02",
        "nested": [
            Decimal("5.50"),
        ],
    }

    result = make_json_safe(value)

    assert result == {
        "datetime": "2026-09-09T18:00:00",
        "date": "2026-09-09",
        "decimal": "123.45",
        "bytes": "0102",
        "nested": [
            "5.50",
        ],
    }

def test_make_json_safe_rejects_unknown_type():
    with pytest.raises(
        TypeError,
        match="Unsupported CDC JSON value type",
    ):
        make_json_safe(object())

def test_load_change_event(
    clean_test_change_events,
):
    batch_id = create_test_batch()
    change_event = build_change_event()

    rows_inserted = load_change_events(
        batch_id,
        [change_event],
    )

    assert rows_inserted == 1

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    event_key,
                    operation,
                    primary_key,
                    before_values,
                    after_values,
                    event_timestamp,
                    commit_timestamp
                FROM cdc.change_event
                WHERE event_key = %s;
                """,
                (
                    change_event["event_key"],
                ),
            )

            row = cursor.fetchone()

    assert row[0] == (
        "binlog.000029:9000:0"
    )
    assert row[1] == "INSERT"

    assert row[2] == {
        "order_id": 900001,
    }

    assert row[3] is None

    assert row[4]["amount"] == "123.45"
    assert row[4]["order_date"] == (
        "2026-09-09"
    )
    assert row[4]["updated_at"] == (
        "2026-09-09T18:00:00"
    )

    assert row[5] == change_event[
        "event_timestamp"
    ]

    assert row[6] == change_event[
        "commit_timestamp"
    ]


def test_load_change_event_is_idempotent(
    clean_test_change_events,
):
    batch_id = create_test_batch()
    change_event = build_change_event()

    first_inserted = load_change_events(
        batch_id,
        [change_event],
    )

    second_inserted = load_change_events(
        batch_id,
        [change_event],
    )

    assert first_inserted == 1
    assert second_inserted == 0

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM cdc.change_event
                WHERE event_key = %s;
                """,
                (
                    change_event["event_key"],
                ),
            )

            event_count = cursor.fetchone()[0]

    assert event_count == 1

def build_transformed_event():
    return {
        "transformed_event_id": 201,
        "batch_id": 88,
        "raw_event_id": 101,
        "event_key":
            "binlog.000040:1100:0",
        "operation": "INSERT",
        "source_schema": "sales",
        "source_table": "categories",
        "primary_key": {
            "category_id": 900050,
        },
        "before_values": None,
        "after_values": {
            "category_id": 900050,
            "category_name": "Load Test",
        },
        "binlog_file": "binlog.000040",
        "event_end_position": 1100,
        "row_index": 0,
        "transaction_id": 9001,
        "commit_position": 1200,
        "event_timestamp": None,
        "commit_timestamp": None,
    }

def test_validate_batch_ready_for_load_rejects_incomplete_transform():
    cursor = MagicMock()

    cursor.fetchone.return_value = (
        3,
        2,
    )

    with pytest.raises(
        RuntimeError,
        match="not ready for LOAD",
    ):
        cdc_load.validate_batch_ready_for_load(
            cursor=cursor,
            batch_id=88,
        )

def test_validate_batch_ready_for_load_accepts_complete_transform():
    cursor = MagicMock()

    cursor.fetchone.return_value = (
        3,
        3,
    )

    result = (
        cdc_load
        .validate_batch_ready_for_load(
            cursor=cursor,
            batch_id=88,
        )
    )

    assert result == {
        "raw_events": 3,
        "transformed_events": 3,
    }

def test_load_transformed_batch_is_atomic(
    monkeypatch,
):
    connection = MagicMock()
    cursor = MagicMock()

    connection.__enter__.return_value = (
        connection
    )

    connection.cursor.return_value\
        .__enter__.return_value = cursor

    transformed_events = [
        build_transformed_event()
    ]

    checkpoint_calls = []

    monkeypatch.setattr(
        cdc_load,
        "get_cdc_checkpoint",
        lambda *args, **kwargs: (
            "binlog.000040",
            1000,
        ),
    )

    monkeypatch.setattr(
        cdc_load,
        "get_postgres_connection",
        lambda: connection,
    )

    monkeypatch.setattr(
        cdc_load,
        "validate_batch_ready_for_load",
        lambda **kwargs: {
            "raw_events": 1,
            "transformed_events": 1,
        },
    )

    monkeypatch.setattr(
        cdc_load,
        "fetch_transformed_events",
        lambda **kwargs: (
            transformed_events
        ),
    )

    monkeypatch.setattr(
        cdc_load,
        "insert_change_events",
        lambda **kwargs: 1,
    )

    def checkpoint(
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
        cdc_load,
        "upsert_cdc_checkpoint",
        checkpoint,
    )

    result = (
        cdc_load.load_transformed_cdc_batch(
            batch_id=88,
            end_binlog_file=(
                "binlog.000040"
            ),
            end_binlog_position=1200,
        )
    )

    assert result[
        "events_loaded"
    ] == 1

    assert result[
        "events_inserted"
    ] == 1

    assert checkpoint_calls == [
        (
            cdc_load.PIPELINE_NAME,
            cdc_load.CHECKPOINT_NAME,
            "binlog.000040",
            1200,
        )
    ]

    connection.commit.assert_called_once_with()
    connection.rollback.assert_not_called()

def test_load_transformed_batch_rolls_back_when_checkpoint_fails(
    monkeypatch,
):
    connection = MagicMock()
    cursor = MagicMock()

    connection.__enter__.return_value = (
        connection
    )

    connection.cursor.return_value\
        .__enter__.return_value = cursor

    monkeypatch.setattr(
        cdc_load,
        "get_cdc_checkpoint",
        lambda *args, **kwargs: (
            "binlog.000040",
            1000,
        ),
    )

    monkeypatch.setattr(
        cdc_load,
        "get_postgres_connection",
        lambda: connection,
    )

    monkeypatch.setattr(
        cdc_load,
        "validate_batch_ready_for_load",
        lambda **kwargs: {
            "raw_events": 1,
            "transformed_events": 1,
        },
    )

    monkeypatch.setattr(
        cdc_load,
        "fetch_transformed_events",
        lambda **kwargs: [
            build_transformed_event()
        ],
    )

    monkeypatch.setattr(
        cdc_load,
        "insert_change_events",
        lambda **kwargs: 1,
    )

    def fail_checkpoint(*args, **kwargs):
        raise RuntimeError(
            "apply checkpoint failure"
        )

    monkeypatch.setattr(
        cdc_load,
        "upsert_cdc_checkpoint",
        fail_checkpoint,
    )

    with pytest.raises(
        RuntimeError,
        match="apply checkpoint failure",
    ):
        cdc_load.load_transformed_cdc_batch(
            batch_id=88,
            end_binlog_file=(
                "binlog.000040"
            ),
            end_binlog_position=1200,
        )

    connection.commit.assert_not_called()
    connection.rollback.assert_called_once_with()

def test_load_empty_batch_advances_apply_checkpoint(
    monkeypatch,
):
    connection = MagicMock()
    cursor = MagicMock()

    connection.__enter__.return_value = (
        connection
    )

    connection.cursor.return_value\
        .__enter__.return_value = cursor

    checkpoint_calls = []

    monkeypatch.setattr(
        cdc_load,
        "get_cdc_checkpoint",
        lambda *args, **kwargs: (
            "binlog.000040",
            1000,
        ),
    )

    monkeypatch.setattr(
        cdc_load,
        "get_postgres_connection",
        lambda: connection,
    )

    monkeypatch.setattr(
        cdc_load,
        "validate_batch_ready_for_load",
        lambda **kwargs: {
            "raw_events": 0,
            "transformed_events": 0,
        },
    )

    monkeypatch.setattr(
        cdc_load,
        "fetch_transformed_events",
        lambda **kwargs: [],
    )

    monkeypatch.setattr(
        cdc_load,
        "insert_change_events",
        lambda **kwargs: 0,
    )

    monkeypatch.setattr(
        cdc_load,
        "upsert_cdc_checkpoint",
        lambda *args: (
            checkpoint_calls.append(
                args
            )
        ),
    )

    result = (
        cdc_load.load_transformed_cdc_batch(
            batch_id=88,
            end_binlog_file=(
                "binlog.000041"
            ),
            end_binlog_position=157,
        )
    )

    assert result["events_loaded"] == 0
    assert result["events_inserted"] == 0

    assert checkpoint_calls

    connection.commit.assert_called_once_with()

def test_load_rejects_apply_checkpoint_regression(
    monkeypatch,
):
    monkeypatch.setattr(
        cdc_load,
        "get_cdc_checkpoint",
        lambda *args, **kwargs: (
            "binlog.000041",
            900,
        ),
    )

    with pytest.raises(
        RuntimeError,
        match="regression rejected",
    ):
        cdc_load.load_transformed_cdc_batch(
            batch_id=88,
            end_binlog_file=(
                "binlog.000041"
            ),
            end_binlog_position=700,
        )

def test_get_transformed_batch_metrics(
    monkeypatch,
):
    connection = MagicMock()
    cursor = MagicMock()

    connection.__enter__.return_value = (
        connection
    )

    connection.cursor.return_value\
        .__enter__.return_value = cursor

    cursor.fetchone.return_value = (
        2,
        5,
        2,
        2,
        1,
    )

    monkeypatch.setattr(
        cdc_load,
        "get_postgres_connection",
        lambda: connection,
    )

    metrics = (
        cdc_load
        .get_transformed_batch_metrics(
            batch_id=88
        )
    )

    assert metrics == {
        "transaction_count": 2,
        "event_count": 5,
        "insert_count": 2,
        "update_count": 2,
        "delete_count": 1,
    }
