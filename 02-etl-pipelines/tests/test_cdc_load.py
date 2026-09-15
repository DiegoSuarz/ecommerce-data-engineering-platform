from datetime import date, datetime, timezone
from decimal import Decimal

from unittest.mock import MagicMock, Mock

import pytest

import cdc_load

from transform import calculate_scd2_hash

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

    monkeypatch.setattr(
        cdc_load,
        "apply_change_events_to_dw",
        lambda cursor, change_events: len(
            change_events
        ),
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

    monkeypatch.setattr(
        cdc_load,
        "apply_change_events_to_dw",
        lambda cursor, change_events: len(
            change_events
        ),
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


def test_load_transformed_batch_requires_dw_apply_before_checkpoint(
    monkeypatch,
):
    connection = MagicMock()
    cursor = MagicMock()

    connection.__enter__.return_value = connection
    connection.cursor.return_value\
        .__enter__.return_value = cursor

    transformed_events = [
        build_transformed_event()
    ]

    call_order = []

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
        lambda **kwargs: transformed_events,
    )

    def insert_final(**kwargs):
        call_order.append("final")
        return 1

    monkeypatch.setattr(
        cdc_load,
        "insert_change_events",
        insert_final,
    )

    def apply_dw(
        cursor,
        change_events,
    ):
        call_order.append("dw")

        assert change_events is (
            transformed_events
        )

        return len(change_events)

    monkeypatch.setattr(
        cdc_load,
        "apply_change_events_to_dw",
        apply_dw,
        raising=False,
    )

    def advance_checkpoint(
        *args,
        **kwargs,
    ):
        call_order.append("checkpoint")

    monkeypatch.setattr(
        cdc_load,
        "upsert_cdc_checkpoint",
        advance_checkpoint,
    )

    cdc_load.load_transformed_cdc_batch(
        batch_id=88,
        end_binlog_file=(
            "binlog.000040"
        ),
        end_binlog_position=1200,
    )

    assert call_order == [
        "final",
        "dw",
        "checkpoint",
    ]

    connection.commit\
        .assert_called_once_with()

    connection.rollback\
        .assert_not_called()


def test_load_transformed_batch_rolls_back_when_dw_apply_fails(
    monkeypatch,
):
    connection = MagicMock()
    cursor = MagicMock()

    connection.__enter__.return_value = connection
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

    def fail_dw_apply(
        cursor,
        change_events,
    ):
        raise RuntimeError(
            "dw apply failure"
        )

    monkeypatch.setattr(
        cdc_load,
        "apply_change_events_to_dw",
        fail_dw_apply,
        raising=False,
    )

    checkpoint_calls = []

    monkeypatch.setattr(
        cdc_load,
        "upsert_cdc_checkpoint",
        lambda *args, **kwargs: (
            checkpoint_calls.append(
                (args, kwargs)
            )
        ),
    )

    with pytest.raises(
        RuntimeError,
        match="dw apply failure",
    ):
        cdc_load.load_transformed_cdc_batch(
            batch_id=88,
            end_binlog_file=(
                "binlog.000040"
            ),
            end_binlog_position=1200,
        )

    assert checkpoint_calls == []

    connection.commit\
        .assert_not_called()

    connection.rollback\
        .assert_called_once_with()


TEST_CATEGORY_ID = 900050
TEST_CATEGORY_CODE = "M9CAT"


def build_category_cdc_event(
    operation,
    *,
    before_values=None,
    after_values=None,
    commit_timestamp=None,
):
    return {
        "operation": operation,
        "source_schema": "sales",
        "source_table": "categories",
        "primary_key": {
            "category_id": TEST_CATEGORY_ID,
        },
        "before_values": before_values,
        "after_values": after_values,
        "commit_timestamp": (
            commit_timestamp
            or datetime(
                2026,
                9,
                14,
                12,
                0,
                0,
                tzinfo=timezone.utc,
            )
        ),
    }


def clean_test_category(cursor):
    cursor.execute(
        """
        DELETE FROM dw.dim_category
        WHERE category_id = %s
           OR category_code = %s;
        """,
        (
            TEST_CATEGORY_ID,
            TEST_CATEGORY_CODE,
        ),
    )


def test_apply_category_insert_creates_current_dimension_version():
    event = build_category_cdc_event(
        "INSERT",
        after_values={
            "category_id": TEST_CATEGORY_ID,
            "category_code": TEST_CATEGORY_CODE,
            "category_name": "M9 Category",
            "department": "M9 Department A",
            "updated_at": (
                "2026-09-14T10:00:00+00:00"
            ),
        },
    )

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                clean_test_category(cursor)

                result = (
                    cdc_load
                    .apply_category_change_event(
                        cursor,
                        event,
                    )
                )

                cursor.execute(
                    """
                    SELECT
                        category_code,
                        category_name,
                        department,
                        effective_from,
                        effective_to,
                        is_current
                    FROM dw.dim_category
                    WHERE category_id = %s;
                    """,
                    (TEST_CATEGORY_ID,),
                )

                rows = cursor.fetchall()

            assert result == 1
            assert len(rows) == 1
            assert rows[0][0] == TEST_CATEGORY_CODE
            assert rows[0][1] == "M9 Category"
            assert rows[0][2] == "M9 Department A"
            assert rows[0][4] is None
            assert rows[0][5] is True

        finally:
            connection.rollback()


def test_apply_category_update_preserves_scd1_and_scd2_semantics():
    insert_event = build_category_cdc_event(
        "INSERT",
        after_values={
            "category_id": TEST_CATEGORY_ID,
            "category_code": TEST_CATEGORY_CODE,
            "category_name": "Original Name",
            "department": "Department A",
            "updated_at": (
                "2026-09-14T10:00:00+00:00"
            ),
        },
    )

    update_event = build_category_cdc_event(
        "UPDATE",
        before_values={
            "category_id": TEST_CATEGORY_ID,
            "category_code": TEST_CATEGORY_CODE,
            "category_name": "Original Name",
            "department": "Department A",
            "updated_at": (
                "2026-09-14T10:00:00+00:00"
            ),
        },
        after_values={
            "category_id": TEST_CATEGORY_ID,
            "category_code": TEST_CATEGORY_CODE,
            "category_name": "Renamed Category",
            "department": "Department B",
            "updated_at": (
                "2026-09-14T11:00:00+00:00"
            ),
        },
    )

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                clean_test_category(cursor)

                cdc_load.apply_category_change_event(
                    cursor,
                    insert_event,
                )

                cdc_load.apply_category_change_event(
                    cursor,
                    update_event,
                )

                cursor.execute(
                    """
                    SELECT
                        category_name,
                        department,
                        effective_from,
                        effective_to,
                        is_current
                    FROM dw.dim_category
                    WHERE category_id = %s
                    ORDER BY effective_from;
                    """,
                    (TEST_CATEGORY_ID,),
                )

                rows = cursor.fetchall()

            assert len(rows) == 2

            assert rows[0][0] == (
                "Renamed Category"
            )
            assert rows[0][1] == (
                "Department A"
            )
            assert rows[0][3] is not None
            assert rows[0][4] is False

            assert rows[1][0] == (
                "Renamed Category"
            )
            assert rows[1][1] == (
                "Department B"
            )
            assert rows[1][3] is None
            assert rows[1][4] is True

        finally:
            connection.rollback()


def test_apply_category_update_rejects_scd0_code_change():
    event = build_category_cdc_event(
        "UPDATE",
        before_values={
            "category_id": TEST_CATEGORY_ID,
            "category_code": TEST_CATEGORY_CODE,
            "category_name": "M9 Category",
            "department": "Department A",
            "updated_at": (
                "2026-09-14T10:00:00+00:00"
            ),
        },
        after_values={
            "category_id": TEST_CATEGORY_ID,
            "category_code": "CHANGED",
            "category_name": "M9 Category",
            "department": "Department A",
            "updated_at": (
                "2026-09-14T11:00:00+00:00"
            ),
        },
    )

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                with pytest.raises(
                    RuntimeError,
                    match="category_code",
                ):
                    (
                        cdc_load
                        .apply_category_change_event(
                            cursor,
                            event,
                        )
                    )
        finally:
            connection.rollback()


def test_apply_category_delete_closes_current_version():
    insert_event = build_category_cdc_event(
        "INSERT",
        after_values={
            "category_id": TEST_CATEGORY_ID,
            "category_code": TEST_CATEGORY_CODE,
            "category_name": "M9 Category",
            "department": "Department A",
            "updated_at": (
                "2026-09-14T10:00:00+00:00"
            ),
        },
    )

    delete_event = build_category_cdc_event(
        "DELETE",
        before_values=insert_event[
            "after_values"
        ],
        commit_timestamp=datetime(
            2026,
            9,
            14,
            12,
            0,
            0,
            tzinfo=timezone.utc,
        ),
    )

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                clean_test_category(cursor)

                cdc_load.apply_category_change_event(
                    cursor,
                    insert_event,
                )

                result = (
                    cdc_load
                    .apply_category_change_event(
                        cursor,
                        delete_event,
                    )
                )

                cursor.execute(
                    """
                    SELECT
                        effective_to,
                        is_current
                    FROM dw.dim_category
                    WHERE category_id = %s;
                    """,
                    (TEST_CATEGORY_ID,),
                )

                row = cursor.fetchone()

            assert result == 1
            assert row[0] is not None
            assert row[1] is False

        finally:
            connection.rollback()


def test_apply_category_scd2_replay_is_idempotent():
    insert_event = build_category_cdc_event(
        "INSERT",
        after_values={
            "category_id": TEST_CATEGORY_ID,
            "category_code": TEST_CATEGORY_CODE,
            "category_name": "M9 Category",
            "department": "Department A",
            "updated_at": (
                "2026-09-14T10:00:00+00:00"
            ),
        },
    )

    update_event = build_category_cdc_event(
        "UPDATE",
        before_values=insert_event[
            "after_values"
        ],
        after_values={
            "category_id": TEST_CATEGORY_ID,
            "category_code": TEST_CATEGORY_CODE,
            "category_name": "M9 Category",
            "department": "Department B",
            "updated_at": (
                "2026-09-14T11:00:00+00:00"
            ),
        },
    )

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                clean_test_category(cursor)

                cdc_load.apply_category_change_event(
                    cursor,
                    insert_event,
                )

                cdc_load.apply_category_change_event(
                    cursor,
                    update_event,
                )

                cdc_load.apply_category_change_event(
                    cursor,
                    update_event,
                )

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM dw.dim_category
                    WHERE category_id = %s;
                    """,
                    (TEST_CATEGORY_ID,),
                )

                version_count = (
                    cursor.fetchone()[0]
                )

            assert version_count == 2

        finally:
            connection.rollback()


def test_apply_change_events_to_dw_dispatches_categories_in_order(
    monkeypatch,
):
    events = [
        build_category_cdc_event(
            "INSERT",
            after_values={
                "category_id": TEST_CATEGORY_ID,
                "category_code": TEST_CATEGORY_CODE,
                "category_name": "Category A",
                "department": "Department A",
                "updated_at": (
                    "2026-09-14T10:00:00+00:00"
                ),
            },
        ),
        build_category_cdc_event(
            "UPDATE",
            before_values={
                "category_id": TEST_CATEGORY_ID,
                "category_code": TEST_CATEGORY_CODE,
                "category_name": "Category A",
                "department": "Department A",
                "updated_at": (
                    "2026-09-14T10:00:00+00:00"
                ),
            },
            after_values={
                "category_id": TEST_CATEGORY_ID,
                "category_code": TEST_CATEGORY_CODE,
                "category_name": "Category B",
                "department": "Department A",
                "updated_at": (
                    "2026-09-14T11:00:00+00:00"
                ),
            },
        ),
    ]

    calls = []

    def apply_category(
        cursor,
        event,
    ):
        calls.append(
            event["operation"]
        )
        return 1

    monkeypatch.setattr(
        cdc_load,
        "apply_category_change_event",
        apply_category,
    )

    cursor = MagicMock()

    result = (
        cdc_load
        .apply_change_events_to_dw(
            cursor,
            events,
        )
    )

    assert result == 2

    assert calls == [
        "INSERT",
        "UPDATE",
    ]


TEST_COUNTRY_ID = 900050
TEST_COUNTRY_CODE = "M9"


def build_country_cdc_event(
    operation,
    *,
    before_values=None,
    after_values=None,
    commit_timestamp=None,
):
    return {
        "operation": operation,
        "source_schema": "sales",
        "source_table": "countries",
        "primary_key": {
            "country_id": TEST_COUNTRY_ID,
        },
        "before_values": before_values,
        "after_values": after_values,
        "commit_timestamp": (
            commit_timestamp
            or datetime(
                2026,
                9,
                14,
                14,
                0,
                0,
                tzinfo=timezone.utc,
            )
        ),
    }


def clean_test_country(cursor):
    cursor.execute(
        """
        DELETE FROM dw.dim_country
        WHERE country_id = %s
           OR country_code = %s;
        """,
        (
            TEST_COUNTRY_ID,
            TEST_COUNTRY_CODE,
        ),
    )


def test_apply_country_insert_creates_current_dimension_version():
    event = build_country_cdc_event(
        "INSERT",
        after_values={
            "country_id": TEST_COUNTRY_ID,
            "country_code": TEST_COUNTRY_CODE,
            "country_name": "M9 Country",
            "sales_region": "LATAM",
            "market_segment": "GROWTH",
            "updated_at": (
                "2026-09-14T10:00:00+00:00"
            ),
        },
    )

    expected_hash = calculate_scd2_hash(
        "LATAM",
        "GROWTH",
    )

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                clean_test_country(cursor)

                result = (
                    cdc_load
                    .apply_country_change_event(
                        cursor,
                        event,
                    )
                )

                cursor.execute(
                    """
                    SELECT
                        country_code,
                        country_name,
                        sales_region,
                        market_segment,
                        row_hash,
                        effective_from,
                        effective_to,
                        is_current
                    FROM dw.dim_country
                    WHERE country_id = %s;
                    """,
                    (TEST_COUNTRY_ID,),
                )

                rows = cursor.fetchall()

            assert result == 1
            assert len(rows) == 1

            assert rows[0][0] == (
                TEST_COUNTRY_CODE
            )
            assert rows[0][1] == "M9 Country"
            assert rows[0][2] == "LATAM"
            assert rows[0][3] == "GROWTH"
            assert rows[0][4] == expected_hash
            assert rows[0][6] is None
            assert rows[0][7] is True

        finally:
            connection.rollback()


def test_apply_country_name_update_is_scd1_only():
    insert_event = build_country_cdc_event(
        "INSERT",
        after_values={
            "country_id": TEST_COUNTRY_ID,
            "country_code": TEST_COUNTRY_CODE,
            "country_name": "Original Country",
            "sales_region": "LATAM",
            "market_segment": "GROWTH",
            "updated_at": (
                "2026-09-14T10:00:00+00:00"
            ),
        },
    )

    update_event = build_country_cdc_event(
        "UPDATE",
        before_values=insert_event[
            "after_values"
        ],
        after_values={
            "country_id": TEST_COUNTRY_ID,
            "country_code": TEST_COUNTRY_CODE,
            "country_name": "Renamed Country",
            "sales_region": "LATAM",
            "market_segment": "GROWTH",
            "updated_at": (
                "2026-09-14T11:00:00+00:00"
            ),
        },
    )

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                clean_test_country(cursor)

                cdc_load.apply_country_change_event(
                    cursor,
                    insert_event,
                )

                cdc_load.apply_country_change_event(
                    cursor,
                    update_event,
                )

                cursor.execute(
                    """
                    SELECT
                        country_name,
                        COUNT(*) OVER ()
                    FROM dw.dim_country
                    WHERE country_id = %s;
                    """,
                    (TEST_COUNTRY_ID,),
                )

                row = cursor.fetchone()

            assert row[0] == "Renamed Country"
            assert row[1] == 1

        finally:
            connection.rollback()


def test_apply_country_update_preserves_scd1_and_scd2_semantics():
    insert_event = build_country_cdc_event(
        "INSERT",
        after_values={
            "country_id": TEST_COUNTRY_ID,
            "country_code": TEST_COUNTRY_CODE,
            "country_name": "Original Country",
            "sales_region": "LATAM",
            "market_segment": "GROWTH",
            "updated_at": (
                "2026-09-14T10:00:00+00:00"
            ),
        },
    )

    update_event = build_country_cdc_event(
        "UPDATE",
        before_values=insert_event[
            "after_values"
        ],
        after_values={
            "country_id": TEST_COUNTRY_ID,
            "country_code": TEST_COUNTRY_CODE,
            "country_name": "Renamed Country",
            "sales_region": "EUROPE",
            "market_segment": "STRATEGIC",
            "updated_at": (
                "2026-09-14T11:00:00+00:00"
            ),
        },
    )

    old_hash = calculate_scd2_hash(
        "LATAM",
        "GROWTH",
    )

    new_hash = calculate_scd2_hash(
        "EUROPE",
        "STRATEGIC",
    )

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                clean_test_country(cursor)

                cdc_load.apply_country_change_event(
                    cursor,
                    insert_event,
                )

                cdc_load.apply_country_change_event(
                    cursor,
                    update_event,
                )

                cursor.execute(
                    """
                    SELECT
                        country_name,
                        sales_region,
                        market_segment,
                        row_hash,
                        effective_from,
                        effective_to,
                        is_current
                    FROM dw.dim_country
                    WHERE country_id = %s
                    ORDER BY effective_from;
                    """,
                    (TEST_COUNTRY_ID,),
                )

                rows = cursor.fetchall()

            assert len(rows) == 2

            assert rows[0][0] == (
                "Renamed Country"
            )
            assert rows[0][1] == "LATAM"
            assert rows[0][2] == "GROWTH"
            assert rows[0][3] == old_hash
            assert rows[0][5] is not None
            assert rows[0][6] is False

            assert rows[1][0] == (
                "Renamed Country"
            )
            assert rows[1][1] == "EUROPE"
            assert rows[1][2] == "STRATEGIC"
            assert rows[1][3] == new_hash
            assert rows[1][5] is None
            assert rows[1][6] is True

        finally:
            connection.rollback()


def test_apply_country_update_rejects_scd0_code_change():
    event = build_country_cdc_event(
        "UPDATE",
        before_values={
            "country_id": TEST_COUNTRY_ID,
            "country_code": TEST_COUNTRY_CODE,
            "country_name": "M9 Country",
            "sales_region": "LATAM",
            "market_segment": "GROWTH",
            "updated_at": (
                "2026-09-14T10:00:00+00:00"
            ),
        },
        after_values={
            "country_id": TEST_COUNTRY_ID,
            "country_code": "X9",
            "country_name": "M9 Country",
            "sales_region": "LATAM",
            "market_segment": "GROWTH",
            "updated_at": (
                "2026-09-14T11:00:00+00:00"
            ),
        },
    )

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                with pytest.raises(
                    RuntimeError,
                    match="country_code",
                ):
                    (
                        cdc_load
                        .apply_country_change_event(
                            cursor,
                            event,
                        )
                    )
        finally:
            connection.rollback()


def test_apply_country_delete_closes_current_version():
    insert_event = build_country_cdc_event(
        "INSERT",
        after_values={
            "country_id": TEST_COUNTRY_ID,
            "country_code": TEST_COUNTRY_CODE,
            "country_name": "M9 Country",
            "sales_region": "LATAM",
            "market_segment": "GROWTH",
            "updated_at": (
                "2026-09-14T10:00:00+00:00"
            ),
        },
    )

    delete_event = build_country_cdc_event(
        "DELETE",
        before_values=insert_event[
            "after_values"
        ],
        commit_timestamp=datetime(
            2026,
            9,
            14,
            12,
            0,
            0,
            tzinfo=timezone.utc,
        ),
    )

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                clean_test_country(cursor)

                cdc_load.apply_country_change_event(
                    cursor,
                    insert_event,
                )

                result = (
                    cdc_load
                    .apply_country_change_event(
                        cursor,
                        delete_event,
                    )
                )

                cursor.execute(
                    """
                    SELECT
                        effective_to,
                        is_current
                    FROM dw.dim_country
                    WHERE country_id = %s;
                    """,
                    (TEST_COUNTRY_ID,),
                )

                row = cursor.fetchone()

            assert result == 1
            assert row[0] is not None
            assert row[1] is False

        finally:
            connection.rollback()


def test_apply_country_scd2_replay_is_idempotent():
    insert_event = build_country_cdc_event(
        "INSERT",
        after_values={
            "country_id": TEST_COUNTRY_ID,
            "country_code": TEST_COUNTRY_CODE,
            "country_name": "M9 Country",
            "sales_region": "LATAM",
            "market_segment": "GROWTH",
            "updated_at": (
                "2026-09-14T10:00:00+00:00"
            ),
        },
    )

    update_event = build_country_cdc_event(
        "UPDATE",
        before_values=insert_event[
            "after_values"
        ],
        after_values={
            "country_id": TEST_COUNTRY_ID,
            "country_code": TEST_COUNTRY_CODE,
            "country_name": "M9 Country",
            "sales_region": "EUROPE",
            "market_segment": "STRATEGIC",
            "updated_at": (
                "2026-09-14T11:00:00+00:00"
            ),
        },
    )

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                clean_test_country(cursor)

                cdc_load.apply_country_change_event(
                    cursor,
                    insert_event,
                )

                cdc_load.apply_country_change_event(
                    cursor,
                    update_event,
                )

                cdc_load.apply_country_change_event(
                    cursor,
                    update_event,
                )

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM dw.dim_country
                    WHERE country_id = %s;
                    """,
                    (TEST_COUNTRY_ID,),
                )

                version_count = (
                    cursor.fetchone()[0]
                )

            assert version_count == 2

        finally:
            connection.rollback()


def test_apply_change_events_to_dw_dispatches_country(
    monkeypatch,
):
    event = build_country_cdc_event(
        "INSERT",
        after_values={
            "country_id": TEST_COUNTRY_ID,
            "country_code": TEST_COUNTRY_CODE,
            "country_name": "M9 Country",
            "sales_region": "LATAM",
            "market_segment": "GROWTH",
            "updated_at": (
                "2026-09-14T10:00:00+00:00"
            ),
        },
    )

    calls = []

    def apply_country(
        cursor,
        received_event,
    ):
        calls.append(
            received_event["operation"]
        )
        return 1

    monkeypatch.setattr(
        cdc_load,
        "apply_country_change_event",
        apply_country,
    )

    cursor = MagicMock()

    result = (
        cdc_load
        .apply_change_events_to_dw(
            cursor,
            [event],
        )
    )

    assert result == 1
    assert calls == ["INSERT"]


TEST_ORDER_ID = 9900050
TEST_ORDER_DATE_1 = "2035-01-15"
TEST_ORDER_DATE_2 = "2035-01-17"

TEST_DATE_KEY_1 = 20350115
TEST_DATE_KEY_2 = 20350117


def build_order_cdc_event(
    operation,
    *,
    before_values=None,
    after_values=None,
    commit_timestamp=None,
):
    return {
        "operation": operation,
        "source_schema": "sales",
        "source_table": "orders",
        "primary_key": {
            "order_id": TEST_ORDER_ID,
        },
        "before_values": before_values,
        "after_values": after_values,
        "commit_timestamp": (
            commit_timestamp
            or datetime(
                2035,
                1,
                18,
                12,
                0,
                0,
                tzinfo=timezone.utc,
            )
        ),
    }


def build_test_order_values(
    *,
    order_date=TEST_ORDER_DATE_1,
    amount="150.25",
    updated_at=(
        "2035-01-15T12:00:00+00:00"
    ),
):
    return {
        "order_id": TEST_ORDER_ID,
        "order_date": order_date,
        "country_id": TEST_COUNTRY_ID,
        "category_id": TEST_CATEGORY_ID,
        "amount": amount,
        "updated_at": updated_at,
    }


def clean_test_order_context(cursor):
    cursor.execute(
        """
        DELETE FROM dw.fact_sales
        WHERE order_id = %s;
        """,
        (TEST_ORDER_ID,),
    )

    cursor.execute(
        """
        DELETE FROM dw.dim_date
        WHERE date_key IN (%s, %s);
        """,
        (
            TEST_DATE_KEY_1,
            TEST_DATE_KEY_2,
        ),
    )

    clean_test_category(cursor)
    clean_test_country(cursor)


def seed_test_order_dimensions(cursor):
    category_event = (
        build_category_cdc_event(
            "INSERT",
            after_values={
                "category_id": (
                    TEST_CATEGORY_ID
                ),
                "category_code": (
                    TEST_CATEGORY_CODE
                ),
                "category_name": (
                    "Order Test Category"
                ),
                "department": (
                    "Department A"
                ),
                "updated_at": (
                    "2035-01-01T00:00:00+00:00"
                ),
            },
        )
    )

    country_event = (
        build_country_cdc_event(
            "INSERT",
            after_values={
                "country_id": (
                    TEST_COUNTRY_ID
                ),
                "country_code": (
                    TEST_COUNTRY_CODE
                ),
                "country_name": (
                    "Order Test Country"
                ),
                "sales_region": "LATAM",
                "market_segment": "GROWTH",
                "updated_at": (
                    "2035-01-01T00:00:00+00:00"
                ),
            },
        )
    )

    cdc_load.apply_category_change_event(
        cursor,
        category_event,
    )

    cdc_load.apply_country_change_event(
        cursor,
        country_event,
    )


def test_apply_order_insert_creates_date_and_fact():
    event = build_order_cdc_event(
        "INSERT",
        after_values=(
            build_test_order_values()
        ),
    )

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                clean_test_order_context(
                    cursor
                )

                seed_test_order_dimensions(
                    cursor
                )

                result = (
                    cdc_load
                    .apply_order_change_event(
                        cursor,
                        event,
                    )
                )

                cursor.execute(
                    """
                    SELECT
                        f.order_id,
                        d.full_date,
                        c.country_id,
                        cat.category_id,
                        f.amount,
                        f.source_updated_at
                    FROM dw.fact_sales AS f
                    INNER JOIN dw.dim_date AS d
                        ON f.date_key = d.date_key
                    INNER JOIN dw.dim_country AS c
                        ON f.country_key =
                           c.country_key
                    INNER JOIN dw.dim_category AS cat
                        ON f.category_key =
                           cat.category_key
                    WHERE f.order_id = %s;
                    """,
                    (TEST_ORDER_ID,),
                )

                row = cursor.fetchone()

            assert result == 1
            assert row is not None
            assert row[0] == TEST_ORDER_ID
            assert str(row[1]) == (
                TEST_ORDER_DATE_1
            )
            assert row[2] == TEST_COUNTRY_ID
            assert row[3] == TEST_CATEGORY_ID
            assert row[4] == Decimal(
                "150.25"
            )

        finally:
            connection.rollback()


def test_apply_order_update_reresolves_historical_dimensions():
    insert_values = (
        build_test_order_values()
    )

    insert_event = (
        build_order_cdc_event(
            "INSERT",
            after_values=insert_values,
        )
    )

    update_values = (
        build_test_order_values(
            order_date=(
                TEST_ORDER_DATE_2
            ),
            amount="275.50",
            updated_at=(
                "2035-01-17T12:00:00+00:00"
            ),
        )
    )

    update_event = (
        build_order_cdc_event(
            "UPDATE",
            before_values=insert_values,
            after_values=update_values,
        )
    )

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                clean_test_order_context(
                    cursor
                )

                seed_test_order_dimensions(
                    cursor
                )

                cdc_load.apply_order_change_event(
                    cursor,
                    insert_event,
                )

                category_update = (
                    build_category_cdc_event(
                        "UPDATE",
                        before_values={
                            "category_id": (
                                TEST_CATEGORY_ID
                            ),
                            "category_code": (
                                TEST_CATEGORY_CODE
                            ),
                            "category_name": (
                                "Order Test Category"
                            ),
                            "department": (
                                "Department A"
                            ),
                            "updated_at": (
                                "2035-01-01T00:00:00+00:00"
                            ),
                        },
                        after_values={
                            "category_id": (
                                TEST_CATEGORY_ID
                            ),
                            "category_code": (
                                TEST_CATEGORY_CODE
                            ),
                            "category_name": (
                                "Order Test Category"
                            ),
                            "department": (
                                "Department B"
                            ),
                            "updated_at": (
                                "2035-01-16T00:00:00+00:00"
                            ),
                        },
                    )
                )

                country_update = (
                    build_country_cdc_event(
                        "UPDATE",
                        before_values={
                            "country_id": (
                                TEST_COUNTRY_ID
                            ),
                            "country_code": (
                                TEST_COUNTRY_CODE
                            ),
                            "country_name": (
                                "Order Test Country"
                            ),
                            "sales_region": (
                                "LATAM"
                            ),
                            "market_segment": (
                                "GROWTH"
                            ),
                            "updated_at": (
                                "2035-01-01T00:00:00+00:00"
                            ),
                        },
                        after_values={
                            "country_id": (
                                TEST_COUNTRY_ID
                            ),
                            "country_code": (
                                TEST_COUNTRY_CODE
                            ),
                            "country_name": (
                                "Order Test Country"
                            ),
                            "sales_region": (
                                "EUROPE"
                            ),
                            "market_segment": (
                                "STRATEGIC"
                            ),
                            "updated_at": (
                                "2035-01-16T00:00:00+00:00"
                            ),
                        },
                    )
                )

                cdc_load.apply_category_change_event(
                    cursor,
                    category_update,
                )

                cdc_load.apply_country_change_event(
                    cursor,
                    country_update,
                )

                result = (
                    cdc_load
                    .apply_order_change_event(
                        cursor,
                        update_event,
                    )
                )

                cursor.execute(
                    """
                    SELECT
                        d.full_date,
                        c.sales_region,
                        c.market_segment,
                        cat.department,
                        f.amount
                    FROM dw.fact_sales AS f
                    INNER JOIN dw.dim_date AS d
                        ON f.date_key = d.date_key
                    INNER JOIN dw.dim_country AS c
                        ON f.country_key =
                           c.country_key
                    INNER JOIN dw.dim_category AS cat
                        ON f.category_key =
                           cat.category_key
                    WHERE f.order_id = %s;
                    """,
                    (TEST_ORDER_ID,),
                )

                row = cursor.fetchone()

            assert result == 1
            assert str(row[0]) == (
                TEST_ORDER_DATE_2
            )
            assert row[1] == "EUROPE"
            assert row[2] == "STRATEGIC"
            assert row[3] == "Department B"
            assert row[4] == Decimal(
                "275.50"
            )

        finally:
            connection.rollback()


def test_apply_order_delete_removes_fact_and_is_idempotent():
    insert_event = (
        build_order_cdc_event(
            "INSERT",
            after_values=(
                build_test_order_values()
            ),
        )
    )

    delete_event = (
        build_order_cdc_event(
            "DELETE",
            before_values=(
                build_test_order_values()
            ),
        )
    )

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                clean_test_order_context(
                    cursor
                )

                seed_test_order_dimensions(
                    cursor
                )

                cdc_load.apply_order_change_event(
                    cursor,
                    insert_event,
                )

                first_result = (
                    cdc_load
                    .apply_order_change_event(
                        cursor,
                        delete_event,
                    )
                )

                second_result = (
                    cdc_load
                    .apply_order_change_event(
                        cursor,
                        delete_event,
                    )
                )

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM dw.fact_sales
                    WHERE order_id = %s;
                    """,
                    (TEST_ORDER_ID,),
                )

                fact_count = (
                    cursor.fetchone()[0]
                )

            assert first_result == 1
            assert second_result == 0
            assert fact_count == 0

        finally:
            connection.rollback()


def test_apply_order_insert_replay_is_idempotent():
    event = build_order_cdc_event(
        "INSERT",
        after_values=(
            build_test_order_values()
        ),
    )

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                clean_test_order_context(
                    cursor
                )

                seed_test_order_dimensions(
                    cursor
                )

                cdc_load.apply_order_change_event(
                    cursor,
                    event,
                )

                cdc_load.apply_order_change_event(
                    cursor,
                    event,
                )

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM dw.fact_sales
                    WHERE order_id = %s;
                    """,
                    (TEST_ORDER_ID,),
                )

                fact_count = (
                    cursor.fetchone()[0]
                )

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM dw.dim_date
                    WHERE date_key = %s;
                    """,
                    (TEST_DATE_KEY_1,),
                )

                date_count = (
                    cursor.fetchone()[0]
                )

            assert fact_count == 1
            assert date_count == 1

        finally:
            connection.rollback()


def test_apply_order_insert_fails_without_historical_dimensions():
    event = build_order_cdc_event(
        "INSERT",
        after_values=(
            build_test_order_values()
        ),
    )

    with get_postgres_connection() as connection:
        try:
            with connection.cursor() as cursor:
                clean_test_order_context(
                    cursor
                )

                with pytest.raises(
                    RuntimeError,
                    match="dimension",
                ):
                    (
                        cdc_load
                        .apply_order_change_event(
                            cursor,
                            event,
                        )
                    )

        finally:
            connection.rollback()


def test_apply_change_events_to_dw_dispatches_order(
    monkeypatch,
):
    event = build_order_cdc_event(
        "INSERT",
        after_values=(
            build_test_order_values()
        ),
    )

    calls = []

    def apply_order(
        cursor,
        received_event,
    ):
        calls.append(
            received_event["operation"]
        )
        return 1

    monkeypatch.setattr(
        cdc_load,
        "apply_order_change_event",
        apply_order,
    )

    cursor = MagicMock()

    result = (
        cdc_load
        .apply_change_events_to_dw(
            cursor,
            [event],
        )
    )

    assert result == 1
    assert calls == ["INSERT"]


def test_get_loaded_batch_metrics_reads_durable_final_events(
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
        3,
        1,
        1,
        1,
    )

    monkeypatch.setattr(
        cdc_load,
        "get_postgres_connection",
        lambda: connection,
    )

    metrics = (
        cdc_load
        .get_loaded_batch_metrics(
            batch_id=217
        )
    )

    assert metrics == {
        "event_count": 3,
        "insert_count": 1,
        "update_count": 1,
        "delete_count": 1,
    }

    cursor.execute.assert_called_once()


def test_load_retry_at_applied_checkpoint_does_not_reapply_dw(
    monkeypatch,
):
    target = (
        "binlog.000050",
        2000,
    )

    monkeypatch.setattr(
        cdc_load,
        "get_cdc_checkpoint",
        lambda *args: target,
    )

    connection = MagicMock()
    cursor = MagicMock()

    connection.__enter__.return_value = (
        connection
    )

    connection.cursor.return_value\
        .__enter__.return_value = cursor

    monkeypatch.setattr(
        cdc_load,
        "get_postgres_connection",
        lambda: connection,
    )

    monkeypatch.setattr(
        cdc_load,
        "validate_batch_ready_for_load",
        lambda cursor, batch_id: {
            "raw_events": 3,
            "transformed_events": 3,
        },
    )

    transformed_events = [
        {"event_key": "event-1"},
        {"event_key": "event-2"},
        {"event_key": "event-3"},
    ]

    monkeypatch.setattr(
        cdc_load,
        "fetch_transformed_events",
        lambda cursor, batch_id: (
            transformed_events
        ),
    )

    insert_final = Mock(
        return_value=0
    )

    apply_dw = Mock(
        return_value=0
    )

    advance_checkpoint = Mock()

    monkeypatch.setattr(
        cdc_load,
        "insert_change_events",
        insert_final,
    )

    monkeypatch.setattr(
        cdc_load,
        "apply_change_events_to_dw",
        apply_dw,
    )

    monkeypatch.setattr(
        cdc_load,
        "upsert_cdc_checkpoint",
        advance_checkpoint,
    )

    monkeypatch.setattr(
        cdc_load,
        "get_loaded_batch_metrics",
        lambda batch_id: {
            "event_count": 3,
            "insert_count": 1,
            "update_count": 1,
            "delete_count": 1,
        },
    )

    result = (
        cdc_load
        .load_transformed_cdc_batch(
            batch_id=217,
            end_binlog_file=target[0],
            end_binlog_position=target[1],
        )
    )

    insert_final.assert_not_called()
    apply_dw.assert_not_called()
    advance_checkpoint.assert_not_called()

    assert result[
        "events_inserted"
    ] == 0

    assert result[
        "events_applied"
    ] == 0

    assert result[
        "events_loaded"
    ] == 3


def test_load_retry_at_applied_checkpoint_rejects_incomplete_final(
    monkeypatch,
):
    target = (
        "binlog.000050",
        2000,
    )

    monkeypatch.setattr(
        cdc_load,
        "get_cdc_checkpoint",
        lambda *args: target,
    )

    connection = MagicMock()
    cursor = MagicMock()

    connection.__enter__.return_value = (
        connection
    )

    connection.cursor.return_value\
        .__enter__.return_value = cursor

    monkeypatch.setattr(
        cdc_load,
        "get_postgres_connection",
        lambda: connection,
    )

    monkeypatch.setattr(
        cdc_load,
        "validate_batch_ready_for_load",
        lambda cursor, batch_id: {
            "raw_events": 3,
            "transformed_events": 3,
        },
    )

    monkeypatch.setattr(
        cdc_load,
        "fetch_transformed_events",
        lambda cursor, batch_id: [
            {"event_key": "event-1"},
            {"event_key": "event-2"},
            {"event_key": "event-3"},
        ],
    )

    monkeypatch.setattr(
        cdc_load,
        "get_loaded_batch_metrics",
        lambda batch_id: {
            "event_count": 2,
            "insert_count": 1,
            "update_count": 1,
            "delete_count": 0,
        },
    )

    insert_final = Mock()
    apply_dw = Mock()
    advance_checkpoint = Mock()

    monkeypatch.setattr(
        cdc_load,
        "insert_change_events",
        insert_final,
    )

    monkeypatch.setattr(
        cdc_load,
        "apply_change_events_to_dw",
        apply_dw,
    )

    monkeypatch.setattr(
        cdc_load,
        "upsert_cdc_checkpoint",
        advance_checkpoint,
    )

    with pytest.raises(
        RuntimeError,
        match="replay reconciliation",
    ):
        (
            cdc_load
            .load_transformed_cdc_batch(
                batch_id=217,
                end_binlog_file=target[0],
                end_binlog_position=target[1],
            )
        )

    insert_final.assert_not_called()
    apply_dw.assert_not_called()
    advance_checkpoint.assert_not_called()
