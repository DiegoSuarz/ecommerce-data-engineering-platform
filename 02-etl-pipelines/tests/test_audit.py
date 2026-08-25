import sys
from pathlib import Path

import pytest

TEST_PIPELINE_NAME = "test_audit_reconciliation"
OTHER_TEST_PIPELINE_NAME = "test_other_pipeline"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = PROJECT_ROOT / "02-etl-pipelines" / "src"

sys.path.insert(0, str(SRC_PATH))

from audit import mark_stale_etl_runs
from db import get_postgres_connection

TEST_PIPELINE_NAME = "test_audit_reconciliation"



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
