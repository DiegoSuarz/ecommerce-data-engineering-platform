from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

import cdc_transform


def build_raw_event(
    operation="INSERT",
):
    timestamp = datetime(
        2026,
        9,
        11,
        23,
        0,
        tzinfo=timezone.utc,
    )

    before_values = None
    after_values = {
        "category_id": 900050,
        "category_name": "Transform Test",
    }

    if operation == "UPDATE":
        before_values = {
            "category_id": 900050,
            "category_name": "Before",
        }

    elif operation == "DELETE":
        before_values = {
            "category_id": 900050,
            "category_name": "Deleted",
        }
        after_values = None

    return {
        "raw_event_id": 101,
        "batch_id": 88,
        "event_key":
            "binlog.000040:1100:0",
        "operation": operation,
        "source_schema": "sales",
        "source_table": "categories",
        "before_values": before_values,
        "after_values": after_values,
        "binlog_file": "binlog.000040",
        "event_end_position": 1100,
        "row_index": 0,
        "transaction_id": 9001,
        "commit_position": 1200,
        "event_timestamp": timestamp,
        "commit_timestamp": timestamp,
    }


def pk_metadata():
    return {
        (
            "sales",
            "categories",
        ): (
            "category_id",
        )
    }


def test_transform_insert_extracts_primary_key():
    transformed = (
        cdc_transform.transform_raw_event(
            build_raw_event("INSERT"),
            pk_metadata(),
        )
    )

    assert transformed[
        "primary_key"
    ] == {
        "category_id": 900050,
    }

    assert transformed[
        "before_values"
    ] is None


def test_transform_update_extracts_primary_key():
    transformed = (
        cdc_transform.transform_raw_event(
            build_raw_event("UPDATE"),
            pk_metadata(),
        )
    )

    assert transformed[
        "primary_key"
    ] == {
        "category_id": 900050,
    }

    assert transformed[
        "before_values"
    ] is not None

    assert transformed[
        "after_values"
    ] is not None


def test_transform_delete_extracts_primary_key_from_before():
    transformed = (
        cdc_transform.transform_raw_event(
            build_raw_event("DELETE"),
            pk_metadata(),
        )
    )

    assert transformed[
        "primary_key"
    ] == {
        "category_id": 900050,
    }

    assert transformed[
        "after_values"
    ] is None


def test_transform_rejects_invalid_semantics():
    raw_event = build_raw_event(
        "INSERT"
    )

    raw_event["before_values"] = {
        "category_id": 900050,
    }

    with pytest.raises(
        ValueError,
        match=(
            "Invalid CDC before/after"
        ),
    ):
        cdc_transform.transform_raw_event(
            raw_event,
            pk_metadata(),
        )


def test_transform_requires_primary_key_metadata():
    with pytest.raises(
        KeyError,
        match=(
            "Primary key metadata not "
            "configured"
        ),
    ):
        cdc_transform.transform_raw_event(
            build_raw_event("INSERT"),
            {},
        )


def test_insert_transformed_events_is_idempotent():
    cursor = MagicMock()

    cursor.fetchone.side_effect = [
        (201,),
        None,
    ]

    first = (
        cdc_transform.transform_raw_event(
            build_raw_event("INSERT"),
            pk_metadata(),
        )
    )

    second = dict(first)
    second["raw_event_id"] = 102
    second["event_key"] = (
        "binlog.000040:1100:1"
    )
    second["row_index"] = 1

    inserted = (
        cdc_transform
        .insert_transformed_events(
            cursor=cursor,
            transformed_events=[
                first,
                second,
            ],
        )
    )

    assert inserted == 1
    assert cursor.execute.call_count == 2

    query = (
        cursor.execute
        .call_args_list[0]
        .args[0]
    )

    assert (
        "ON CONFLICT (event_key)"
        in query
    )


def test_transform_cdc_batch_commits(
    monkeypatch,
):
    connection = MagicMock()
    cursor = MagicMock()

    connection.__enter__.return_value = (
        connection
    )

    connection.cursor.return_value\
        .__enter__.return_value = cursor

    raw_events = [
        build_raw_event("INSERT"),
    ]

    monkeypatch.setattr(
        cdc_transform,
        "get_postgres_connection",
        lambda: connection,
    )

    monkeypatch.setattr(
        cdc_transform,
        "fetch_raw_events",
        lambda **kwargs: raw_events,
    )

    monkeypatch.setattr(
        cdc_transform,
        "insert_transformed_events",
        lambda **kwargs: 1,
    )

    result = (
        cdc_transform.transform_cdc_batch(
            batch_id=88
        )
    )

    assert result == {
        "batch_id": 88,
        "raw_events_read": 1,
        "events_transformed": 1,
        "transformed_events_inserted": 1,
    }

    connection.commit.assert_called_once_with()
    connection.rollback.assert_not_called()


def test_transform_cdc_batch_rolls_back_on_failure(
    monkeypatch,
):
    connection = MagicMock()

    connection.__enter__.return_value = (
        connection
    )

    cursor = MagicMock()

    connection.cursor.return_value\
        .__enter__.return_value = cursor

    monkeypatch.setattr(
        cdc_transform,
        "get_postgres_connection",
        lambda: connection,
    )

    monkeypatch.setattr(
        cdc_transform,
        "fetch_raw_events",
        lambda **kwargs: [
            build_raw_event("INSERT")
        ],
    )

    def fail_insert(**kwargs):
        raise RuntimeError(
            "transform insert failure"
        )

    monkeypatch.setattr(
        cdc_transform,
        "insert_transformed_events",
        fail_insert,
    )

    with pytest.raises(
        RuntimeError,
        match="transform insert failure",
    ):
        cdc_transform.transform_cdc_batch(
            batch_id=88
        )

    connection.commit.assert_not_called()
    connection.rollback.assert_called_once_with()
