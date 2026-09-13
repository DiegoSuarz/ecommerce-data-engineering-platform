import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import audit

TEST_PIPELINE_NAME = "test_audit_reconciliation"
OTHER_TEST_PIPELINE_NAME = "test_other_pipeline"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = PROJECT_ROOT / "02-etl-pipelines" / "src"

sys.path.insert(0, str(SRC_PATH))

from audit import mark_stale_etl_runs
from db import get_postgres_connection

from audit import (
    get_cdc_checkpoint,
    initialize_cdc_checkpoint,
    mark_stale_etl_runs,
    update_cdc_checkpoint,
)

from audit import (
    complete_cdc_batch,
    get_cdc_checkpoint,
    initialize_cdc_checkpoint,
    mark_stale_etl_runs,
    start_cdc_batch,
    update_cdc_checkpoint,
)

TEST_PIPELINE_NAME = "test_audit_reconciliation"

TEST_CDC_PIPELINE_NAME = "test_change_data_capture"
TEST_CDC_CHECKPOINT_NAME = "test_mysql_binlog"

TEST_CDC_BATCH_PIPELINE_NAME = (
    "test_cdc_batch_pipeline"
)

@pytest.fixture
def clean_test_cdc_batch():
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
                (TEST_CDC_BATCH_PIPELINE_NAME,),
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
                (TEST_CDC_BATCH_PIPELINE_NAME,),
            )

            cursor.execute(
                """
                DELETE FROM audit.etl_run
                WHERE pipeline_name = %s;
                """,
                (TEST_CDC_BATCH_PIPELINE_NAME,),
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
                (TEST_CDC_BATCH_PIPELINE_NAME,),
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
                (TEST_CDC_BATCH_PIPELINE_NAME,),
            )

            cursor.execute(
                """
                DELETE FROM audit.etl_run
                WHERE pipeline_name = %s;
                """,
                (TEST_CDC_BATCH_PIPELINE_NAME,),
            )

        connection.commit()

@pytest.fixture
def clean_test_audit_runs():
    yield

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM audit.etl_run
                WHERE pipeline_name IN (%s, %s);
                """,
                (
                    TEST_PIPELINE_NAME,
                    OTHER_TEST_PIPELINE_NAME,
                ),
            )

        connection.commit()

class TestCdcBatch:
    def create_test_run(self):
        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO audit.etl_run
                    (
                        pipeline_name,
                        status
                    )
                    VALUES (%s, 'RUNNING')
                    RETURNING run_id;
                    """,
                    (
                        TEST_CDC_BATCH_PIPELINE_NAME,
                    ),
                )

                run_id = cursor.fetchone()[0]

            connection.commit()

        return run_id

    def test_start_creates_batch(
        self,
        clean_test_cdc_batch,
    ):
        run_id = self.create_test_run()

        batch_id = start_cdc_batch(
            run_id,
            "binlog.000029",
            8697,
        )

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        run_id,
                        start_binlog_file,
                        start_binlog_position
                    FROM audit.cdc_batch
                    WHERE batch_id = %s;
                    """,
                    (batch_id,),
                )

                row = cursor.fetchone()

        assert row == (
            run_id,
            "binlog.000029",
            8697,
        )

    def test_start_is_idempotent_for_run_id(
        self,
        clean_test_cdc_batch,
    ):
        run_id = self.create_test_run()

        first_batch_id = start_cdc_batch(
            run_id,
            "binlog.000029",
            8697,
        )

        second_batch_id = start_cdc_batch(
            run_id,
            "binlog.999999",
            999999,
        )

        assert second_batch_id == first_batch_id

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        start_binlog_file,
                        start_binlog_position
                    FROM audit.cdc_batch
                    WHERE batch_id = %s;
                    """,
                    (first_batch_id,),
                )

                row = cursor.fetchone()

        assert row == (
            "binlog.000029",
            8697,
        )

    def test_complete_updates_batch_metrics(
        self,
        clean_test_cdc_batch,
    ):
        run_id = self.create_test_run()

        batch_id = start_cdc_batch(
            run_id,
            "binlog.000029",
            8697,
        )

        complete_cdc_batch(
            batch_id=batch_id,
            end_binlog_file="binlog.000029",
            end_binlog_position=9500,
            transactions_processed=3,
            events_processed=7,
            insert_events=3,
            update_events=2,
            delete_events=2,
        )

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        end_binlog_file,
                        end_binlog_position,
                        transactions_processed,
                        events_processed,
                        insert_events,
                        update_events,
                        delete_events
                    FROM audit.cdc_batch
                    WHERE batch_id = %s;
                    """,
                    (batch_id,),
                )

                row = cursor.fetchone()

        assert row == (
            "binlog.000029",
            9500,
            3,
            7,
            3,
            2,
            2,
        )

    def test_complete_is_idempotent(
        self,
        clean_test_cdc_batch,
    ):
        run_id = self.create_test_run()

        batch_id = start_cdc_batch(
            run_id,
            "binlog.000029",
            8697,
        )

        kwargs = {
            "batch_id": batch_id,
            "end_binlog_file": "binlog.000029",
            "end_binlog_position": 9500,
            "transactions_processed": 3,
            "events_processed": 7,
            "insert_events": 3,
            "update_events": 2,
            "delete_events": 2,
        }

        complete_cdc_batch(**kwargs)
        complete_cdc_batch(**kwargs)

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        transactions_processed,
                        events_processed,
                        insert_events,
                        update_events,
                        delete_events
                    FROM audit.cdc_batch
                    WHERE batch_id = %s;
                    """,
                    (batch_id,),
                )

                row = cursor.fetchone()

        assert row == (
            3,
            7,
            3,
            2,
            2,
        )

class TestMarkStaleEtlRuns:
    """
    Integration tests for audit.mark_stale_etl_runs().
    """
    def test_keeps_recent_running_pipeline(
        self,
        clean_test_audit_runs,
    ):
        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO audit.etl_run
                    (
                        pipeline_name,
                        started_at,
                        status
                    )
                    VALUES
                    (
                        %s,
                        CURRENT_TIMESTAMP - INTERVAL '5 minutes',
                        'RUNNING'
                    )
                    RETURNING run_id;
                    """,
                    (TEST_PIPELINE_NAME,),
                )

                run_id = cursor.fetchone()[0]

            connection.commit()

        updated_run_ids = mark_stale_etl_runs(
            pipeline_name=TEST_PIPELINE_NAME,
            stale_after_minutes=15,
        )

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        status,
                        finished_at,
                        error_message
                    FROM audit.etl_run
                    WHERE run_id = %s;
                    """,
                    (run_id,),
                )

                row = cursor.fetchone()

        assert run_id not in updated_run_ids
        assert row[0] == "RUNNING"
        assert row[1] is None
        assert row[2] is None

    def test_marks_stale_running_pipeline(
    self,
    clean_test_audit_runs,
    ):
        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO audit.etl_run
                    (
                        pipeline_name,
                        started_at,
                        status
                    )
                    VALUES
                    (
                        %s,
                        CURRENT_TIMESTAMP - INTERVAL '30 minutes',
                        'RUNNING'
                    )
                    RETURNING run_id;
                    """,
                    (TEST_PIPELINE_NAME,),
                )

                run_id = cursor.fetchone()[0]

            connection.commit()

        updated_run_ids = mark_stale_etl_runs(
            pipeline_name=TEST_PIPELINE_NAME,
            stale_after_minutes=15,
        )

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        status,
                        finished_at,
                        error_message
                    FROM audit.etl_run
                    WHERE run_id = %s;
                    """,
                    (run_id,),
                )

                row = cursor.fetchone()

        assert run_id in updated_run_ids
        assert row[0] == "FAILED"
        assert row[1] is not None
        assert (
            row[2]
            == "Marked as stale: execution ended without final audit status"
        )

    def test_does_not_touch_other_pipeline(
        self,
        clean_test_audit_runs,
    ):
        other_pipeline_name = OTHER_TEST_PIPELINE_NAME

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO audit.etl_run
                    (
                        pipeline_name,
                        started_at,
                        status
                    )
                    VALUES
                    (
                        %s,
                        CURRENT_TIMESTAMP - INTERVAL '30 minutes',
                        'RUNNING'
                    )
                    RETURNING run_id;
                    """,
                    (other_pipeline_name,),
                )

                run_id = cursor.fetchone()[0]

            connection.commit()

        updated_run_ids = mark_stale_etl_runs(
            pipeline_name=TEST_PIPELINE_NAME,
            stale_after_minutes=15,
        )

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        status,
                        finished_at,
                        error_message
                    FROM audit.etl_run
                    WHERE run_id = %s;
                    """,
                    (run_id,),
                )

                row = cursor.fetchone()

        assert run_id not in updated_run_ids
        assert row[0] == "RUNNING"
        assert row[1] is None
        assert row[2] is None

    def test_does_not_touch_other_pipeline(
    self,
    clean_test_audit_runs,
    ):
        other_pipeline_name = OTHER_TEST_PIPELINE_NAME

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO audit.etl_run
                    (
                        pipeline_name,
                        started_at,
                        status
                    )
                    VALUES
                    (
                        %s,
                        CURRENT_TIMESTAMP - INTERVAL '30 minutes',
                        'RUNNING'
                    )
                    RETURNING run_id;
                    """,
                    (other_pipeline_name,),
                )

                run_id = cursor.fetchone()[0]

            connection.commit()

        updated_run_ids = mark_stale_etl_runs(
            pipeline_name=TEST_PIPELINE_NAME,
            stale_after_minutes=15,
        )

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        status,
                        finished_at,
                        error_message
                    FROM audit.etl_run
                    WHERE run_id = %s;
                    """,
                    (run_id,),
                )

                row = cursor.fetchone()

                cursor.execute(
                    """
                    DELETE FROM audit.etl_run
                    WHERE run_id = %s;
                    """,
                    (run_id,),
                )

            connection.commit()

        assert run_id not in updated_run_ids
        assert row[0] == "RUNNING"
        assert row[1] is None
        assert row[2] is None

    def test_returns_updated_run_ids(
    self,
    clean_test_audit_runs,
    ):
        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO audit.etl_run
                    (
                        pipeline_name,
                        started_at,
                        status
                    )
                    VALUES
                        (
                            %s,
                            CURRENT_TIMESTAMP - INTERVAL '30 minutes',
                            'RUNNING'
                        ),
                        (
                            %s,
                            CURRENT_TIMESTAMP - INTERVAL '20 minutes',
                            'RUNNING'
                        ),
                        (
                            %s,
                            CURRENT_TIMESTAMP - INTERVAL '5 minutes',
                            'RUNNING'
                        )
                    RETURNING run_id;
                    """,
                    (
                        TEST_PIPELINE_NAME,
                        TEST_PIPELINE_NAME,
                        TEST_PIPELINE_NAME,
                    ),
                )

                run_ids = [
                    row[0]
                    for row in cursor.fetchall()
                ]

            connection.commit()

        stale_run_ids = set(run_ids[:2])
        recent_run_id = run_ids[2]

        updated_run_ids = mark_stale_etl_runs(
            pipeline_name=TEST_PIPELINE_NAME,
            stale_after_minutes=15,
        )

        assert set(updated_run_ids) == stale_run_ids
        assert recent_run_id not in updated_run_ids

@pytest.fixture
def clean_test_cdc_checkpoint():
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM audit.cdc_checkpoint
                WHERE pipeline_name = %s
                  AND checkpoint_name = %s;
                """,
                (
                    TEST_CDC_PIPELINE_NAME,
                    TEST_CDC_CHECKPOINT_NAME,
                ),
            )

        connection.commit()

    yield

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM audit.cdc_checkpoint
                WHERE pipeline_name = %s
                  AND checkpoint_name = %s;
                """,
                (
                    TEST_CDC_PIPELINE_NAME,
                    TEST_CDC_CHECKPOINT_NAME,
                ),
            )

        connection.commit()

class TestCdcCheckpoint:
    def test_get_returns_none_when_missing(
        self,
        clean_test_cdc_checkpoint,
    ):
        checkpoint = get_cdc_checkpoint(
            TEST_CDC_PIPELINE_NAME,
            TEST_CDC_CHECKPOINT_NAME,
        )

        assert checkpoint is None

    def test_initialize_creates_checkpoint(
        self,
        clean_test_cdc_checkpoint,
    ):
        checkpoint = initialize_cdc_checkpoint(
            TEST_CDC_PIPELINE_NAME,
            TEST_CDC_CHECKPOINT_NAME,
            "binlog.000028",
            10155,
        )

        assert checkpoint == (
            "binlog.000028",
            10155,
        )

    def test_initialize_does_not_overwrite_existing(
        self,
        clean_test_cdc_checkpoint,
    ):
        initialize_cdc_checkpoint(
            TEST_CDC_PIPELINE_NAME,
            TEST_CDC_CHECKPOINT_NAME,
            "binlog.000028",
            10155,
        )

        checkpoint = initialize_cdc_checkpoint(
            TEST_CDC_PIPELINE_NAME,
            TEST_CDC_CHECKPOINT_NAME,
            "binlog.000029",
            500,
        )

        assert checkpoint == (
            "binlog.000028",
            10155,
        )

    def test_update_advances_existing_checkpoint(
        self,
        clean_test_cdc_checkpoint,
    ):
        initialize_cdc_checkpoint(
            TEST_CDC_PIPELINE_NAME,
            TEST_CDC_CHECKPOINT_NAME,
            "binlog.000028",
            10155,
        )

        update_cdc_checkpoint(
            TEST_CDC_PIPELINE_NAME,
            TEST_CDC_CHECKPOINT_NAME,
            "binlog.000028",
            12000,
        )

        checkpoint = get_cdc_checkpoint(
            TEST_CDC_PIPELINE_NAME,
            TEST_CDC_CHECKPOINT_NAME,
        )

        assert checkpoint == (
            "binlog.000028",
            12000,
        )

    def test_update_can_cross_binlog_files(
        self,
        clean_test_cdc_checkpoint,
    ):
        initialize_cdc_checkpoint(
            TEST_CDC_PIPELINE_NAME,
            TEST_CDC_CHECKPOINT_NAME,
            "binlog.000028",
            10155,
        )

        update_cdc_checkpoint(
            TEST_CDC_PIPELINE_NAME,
            TEST_CDC_CHECKPOINT_NAME,
            "binlog.000029",
            157,
        )

        checkpoint = get_cdc_checkpoint(
            TEST_CDC_PIPELINE_NAME,
            TEST_CDC_CHECKPOINT_NAME,
        )

        assert checkpoint == (
            "binlog.000029",
            157,
        )

def test_initializes_cdc_checkpoint_from_checkpoint(
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
        "binlog.000032",
        2816,
    )

    monkeypatch.setattr(
        audit,
        "get_postgres_connection",
        lambda: connection,
    )

    checkpoint = (
        audit
        .initialize_cdc_checkpoint_from_checkpoint(
            "change_data_capture",
            "mysql_sales_binlog",
            "mysql_sales_binlog_read",
        )
    )

    assert checkpoint == (
        "binlog.000032",
        2816,
    )

    assert cursor.execute.call_count == 1
    connection.commit.assert_called_once_with()
    connection.rollback.assert_not_called()


def test_initialize_cdc_checkpoint_from_checkpoint_is_idempotent(
    monkeypatch,
):
    connection = MagicMock()
    cursor = MagicMock()

    connection.__enter__.return_value = (
        connection
    )

    connection.cursor.return_value\
        .__enter__.return_value = cursor

    cursor.fetchone.side_effect = [
        None,
        (
            "binlog.000033",
            5000,
        ),
    ]

    monkeypatch.setattr(
        audit,
        "get_postgres_connection",
        lambda: connection,
    )

    checkpoint = (
        audit
        .initialize_cdc_checkpoint_from_checkpoint(
            "change_data_capture",
            "mysql_sales_binlog",
            "mysql_sales_binlog_read",
        )
    )

    assert checkpoint == (
        "binlog.000033",
        5000,
    )

    assert cursor.execute.call_count == 2
    connection.commit.assert_called_once_with()
    connection.rollback.assert_not_called()


def test_initialize_cdc_checkpoint_from_checkpoint_requires_source(
    monkeypatch,
):
    connection = MagicMock()
    cursor = MagicMock()

    connection.__enter__.return_value = (
        connection
    )

    connection.cursor.return_value\
        .__enter__.return_value = cursor

    cursor.fetchone.side_effect = [
        None,
        None,
    ]

    monkeypatch.setattr(
        audit,
        "get_postgres_connection",
        lambda: connection,
    )

    with pytest.raises(
        RuntimeError,
        match=(
            "source checkpoint does not exist"
        ),
    ):
        (
            audit
            .initialize_cdc_checkpoint_from_checkpoint(
                "change_data_capture",
                "mysql_sales_binlog",
                "mysql_sales_binlog_read",
            )
        )

    connection.commit.assert_not_called()
    connection.rollback.assert_called_once_with()
