from datetime import datetime, timezone

import pytest

import cdc_load

from audit import (
    get_cdc_checkpoint,
    initialize_cdc_checkpoint,
    start_cdc_batch,
    start_etl_run,
)
from cdc_load import persist_cdc_transaction
from db import get_postgres_connection

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

TEST_PIPELINE_NAME = (
    "test_cdc_atomic_transaction"
)

TEST_CHECKPOINT_NAME = (
    "test_mysql_binlog"
)


@pytest.fixture
def clean_test_cdc_transaction():
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

            cursor.execute(
                """
                DELETE FROM audit.cdc_checkpoint
                WHERE pipeline_name = %s
                  AND checkpoint_name = %s;
                """,
                (
                    TEST_PIPELINE_NAME,
                    TEST_CHECKPOINT_NAME,
                ),
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

            cursor.execute(
                """
                DELETE FROM audit.cdc_checkpoint
                WHERE pipeline_name = %s
                  AND checkpoint_name = %s;
                """,
                (
                    TEST_PIPELINE_NAME,
                    TEST_CHECKPOINT_NAME,
                ),
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


def build_transaction():
    event_timestamp = datetime(
        2026,
        9,
        9,
        18,
        0,
        0,
        tzinfo=timezone.utc,
    )

    commit_timestamp = datetime(
        2026,
        9,
        9,
        18,
        0,
        1,
        tzinfo=timezone.utc,
    )

    return {
        "transaction_id": 5000,
        "binlog_file": "binlog.000029",
        "commit_position": 9031,
        "commit_timestamp": commit_timestamp,
        "change_events": [
            {
                "event_key":
                    "binlog.000029:9000:0",
                "operation": "INSERT",
                "source_schema": "sales",
                "source_table": "categories",
                "primary_key": {
                    "category_id": 900010,
                },
                "before_values": None,
                "after_values": {
                    "category_id": 900010,
                    "category_name":
                        "Atomic CDC Probe",
                },
                "binlog_file":
                    "binlog.000029",
                "event_end_position": 9000,
                "row_index": 0,
                "transaction_id": 5000,
                "commit_position": 9031,
                "event_timestamp":
                    event_timestamp,
                "commit_timestamp":
                    commit_timestamp,
            }
        ],
    }


def test_persist_transaction_updates_event_and_checkpoint(
    clean_test_cdc_transaction,
):
    initialize_cdc_checkpoint(
        TEST_PIPELINE_NAME,
        TEST_CHECKPOINT_NAME,
        "binlog.000029",
        8697,
    )

    batch_id = create_test_batch()
    transaction = build_transaction()

    rows_inserted = persist_cdc_transaction(
        batch_id=batch_id,
        transaction=transaction,
        pipeline_name=TEST_PIPELINE_NAME,
        checkpoint_name=TEST_CHECKPOINT_NAME,
    )

    assert rows_inserted == 1

    checkpoint = get_cdc_checkpoint(
        TEST_PIPELINE_NAME,
        TEST_CHECKPOINT_NAME,
    )

    assert checkpoint == (
        "binlog.000029",
        9031,
    )

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM cdc.change_event
                WHERE event_key = %s;
                """,
                (
                    "binlog.000029:9000:0",
                ),
            )

            event_count = cursor.fetchone()[0]

    assert event_count == 1

def test_checkpoint_failure_rolls_back_events(
    clean_test_cdc_transaction,
    monkeypatch,
):
    initialize_cdc_checkpoint(
        TEST_PIPELINE_NAME,
        TEST_CHECKPOINT_NAME,
        "binlog.000029",
        8697,
    )

    batch_id = create_test_batch()
    transaction = build_transaction()

    def fail_checkpoint(*args, **kwargs):
        raise RuntimeError(
            "Forced checkpoint failure"
        )

    monkeypatch.setattr(
        cdc_load,
        "upsert_cdc_checkpoint",
        fail_checkpoint,
    )

    with pytest.raises(
        RuntimeError,
        match="Forced checkpoint failure",
    ):
        persist_cdc_transaction(
            batch_id=batch_id,
            transaction=transaction,
            pipeline_name=TEST_PIPELINE_NAME,
            checkpoint_name=(
                TEST_CHECKPOINT_NAME
            ),
        )

    checkpoint = get_cdc_checkpoint(
        TEST_PIPELINE_NAME,
        TEST_CHECKPOINT_NAME,
    )

    assert checkpoint == (
        "binlog.000029",
        8697,
    )

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM cdc.change_event
                WHERE event_key = %s;
                """,
                (
                    "binlog.000029:9000:0",
                ),
            )

            event_count = cursor.fetchone()[0]

    assert event_count == 0


def test_persist_transaction_is_idempotent(
    clean_test_cdc_transaction,
):
    initialize_cdc_checkpoint(
        TEST_PIPELINE_NAME,
        TEST_CHECKPOINT_NAME,
        "binlog.000029",
        8697,
    )

    batch_id = create_test_batch()
    transaction = build_transaction()

    first_inserted = persist_cdc_transaction(
        batch_id=batch_id,
        transaction=transaction,
        pipeline_name=TEST_PIPELINE_NAME,
        checkpoint_name=TEST_CHECKPOINT_NAME,
    )

    second_inserted = persist_cdc_transaction(
        batch_id=batch_id,
        transaction=transaction,
        pipeline_name=TEST_PIPELINE_NAME,
        checkpoint_name=TEST_CHECKPOINT_NAME,
    )

    assert first_inserted == 1
    assert second_inserted == 0

    checkpoint = get_cdc_checkpoint(
        TEST_PIPELINE_NAME,
        TEST_CHECKPOINT_NAME,
    )

    assert checkpoint == (
        "binlog.000029",
        9031,
    )

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM cdc.change_event
                WHERE event_key = %s;
                """,
                (
                    "binlog.000029:9000:0",
                ),
            )

            event_count = cursor.fetchone()[0]

    assert event_count == 1
