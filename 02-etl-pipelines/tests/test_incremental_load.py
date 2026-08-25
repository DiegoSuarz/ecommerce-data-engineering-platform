import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

import incremental_load


WATERMARK_TIMESTAMP = datetime(
    2022,
    1,
    1,
    12,
    0,
    0,
    tzinfo=timezone.utc,
)

NEW_WATERMARK_TIMESTAMP = datetime(
    2022,
    1,
    2,
    12,
    0,
    0,
    tzinfo=timezone.utc,
)


def test_incremental_load_no_new_data(monkeypatch):
    completed = {}
    watermark_updated = {"value": False}

    monkeypatch.setattr(
        incremental_load,
        "start_etl_run",
        lambda pipeline_name, **kwargs: 100,
    )

    monkeypatch.setattr(
        incremental_load,
        "get_watermark",
        lambda pipeline_name, watermark_name: (
            WATERMARK_TIMESTAMP,
            300010,
        ),
    )

    monkeypatch.setattr(
        incremental_load,
        "extract_incremental_orders",
        lambda watermark_timestamp,
               watermark_order_id,
               batch_size: iter(()),
    )

    monkeypatch.setattr(
        incremental_load,
        "load_incremental_orders",
        lambda batches: (0, None),
    )

    def fake_complete_etl_run(
        run_id,
        rows_extracted,
        rows_loaded,
        rows_rejected,
    ):
        completed["run_id"] = run_id
        completed["rows_extracted"] = rows_extracted
        completed["rows_loaded"] = rows_loaded
        completed["rows_rejected"] = rows_rejected

    monkeypatch.setattr(
        incremental_load,
        "complete_etl_run",
        fake_complete_etl_run,
    )

    def fake_update_watermark(*args, **kwargs):
        watermark_updated["value"] = True

    monkeypatch.setattr(
        incremental_load,
        "update_watermark",
        fake_update_watermark,
    )

    incremental_load.run_incremental_load()

    assert completed["run_id"] == 100
    assert completed["rows_extracted"] == 0
    assert completed["rows_loaded"] == 0
    assert completed["rows_rejected"] == 0

    assert watermark_updated["value"] is False


def test_incremental_load_updates_watermark_on_success(
    monkeypatch,
):
    completed = {}
    watermark_update = {}

    monkeypatch.setattr(
        incremental_load,
        "start_etl_run",
        lambda pipeline_name, **kwargs: 101,
    )

    monkeypatch.setattr(
        incremental_load,
        "get_watermark",
        lambda pipeline_name, watermark_name: (
            WATERMARK_TIMESTAMP,
            300010,
        ),
    )

    monkeypatch.setattr(
        incremental_load,
        "extract_incremental_orders",
        lambda watermark_timestamp,
               watermark_order_id,
               batch_size: iter(
            [
                [
                    (
                        300011,
                        None,
                        1,
                        1,
                        100,
                        NEW_WATERMARK_TIMESTAMP,
                    ),
                    (
                        300012,
                        None,
                        1,
                        1,
                        200,
                        NEW_WATERMARK_TIMESTAMP,
                    ),
                    (
                        300013,
                        None,
                        1,
                        1,
                        300,
                        NEW_WATERMARK_TIMESTAMP,
                    ),
                    (
                        300014,
                        None,
                        1,
                        1,
                        400,
                        NEW_WATERMARK_TIMESTAMP,
                    ),
                    (
                        300015,
                        None,
                        1,
                        1,
                        500,
                        NEW_WATERMARK_TIMESTAMP,
                    ),
                ]
            ]
        ),
    )

    monkeypatch.setattr(
        incremental_load,
        "load_incremental_orders",
        lambda batches: (
            5,
            (
                NEW_WATERMARK_TIMESTAMP,
                300015,
            ),
        ),
    )

    monkeypatch.setattr(
        incremental_load,
        "run_staging_quality_checks",
        lambda: {},
    )

    monkeypatch.setattr(
        incremental_load,
        "extract_distinct_order_dates",
        lambda: [],
    )

    monkeypatch.setattr(
        incremental_load,
        "build_date_dimension_rows",
        lambda dates: [],
    )

    monkeypatch.setattr(
        incremental_load,
        "load_incremental_dim_date",
        lambda rows: None,
    )

    monkeypatch.setattr(
        incremental_load,
        "load_incremental_fact_sales",
        lambda: None,
    )

    monkeypatch.setattr(
        incremental_load,
        "run_dw_quality_checks",
        lambda: {},
    )

    monkeypatch.setattr(
        incremental_load,
        "reconcile_incremental_batch",
        lambda: {
            "staging_rows": 5,
            "dw_rows": 5,
            "staging_amount": 1500,
            "dw_amount": 1500,
        },
    )

    def fake_update_watermark(
        pipeline_name,
        watermark_name,
        watermark_timestamp,
        watermark_order_id,
    ):
        watermark_update["pipeline_name"] = (
            pipeline_name
        )
        watermark_update["watermark_name"] = (
            watermark_name
        )
        watermark_update["watermark_timestamp"] = (
            watermark_timestamp
        )
        watermark_update["watermark_order_id"] = (
            watermark_order_id
        )

    monkeypatch.setattr(
        incremental_load,
        "update_watermark",
        fake_update_watermark,
    )

    def fake_complete_etl_run(
        run_id,
        rows_extracted,
        rows_loaded,
        rows_rejected,
    ):
        completed["run_id"] = run_id
        completed["rows_extracted"] = rows_extracted
        completed["rows_loaded"] = rows_loaded
        completed["rows_rejected"] = rows_rejected

    monkeypatch.setattr(
        incremental_load,
        "complete_etl_run",
        fake_complete_etl_run,
    )

    incremental_load.run_incremental_load()

    assert (
        watermark_update["watermark_timestamp"]
        == NEW_WATERMARK_TIMESTAMP
    )

    assert (
        watermark_update["watermark_order_id"]
        == 300015
    )

    assert completed["run_id"] == 101
    assert completed["rows_extracted"] == 5
    assert completed["rows_loaded"] == 5
    assert completed["rows_rejected"] == 0


def test_incremental_load_does_not_update_watermark_on_failure(
    monkeypatch,
):
    failed = {}
    watermark_updated = {"value": False}

    monkeypatch.setattr(
        incremental_load,
        "start_etl_run",
        lambda pipeline_name, **kwargs: 102,
    )

    monkeypatch.setattr(
        incremental_load,
        "get_watermark",
        lambda pipeline_name, watermark_name: (
            WATERMARK_TIMESTAMP,
            300010,
        ),
    )

    monkeypatch.setattr(
        incremental_load,
        "extract_incremental_orders",
        lambda watermark_timestamp,
               watermark_order_id,
               batch_size: iter(
            [
                [
                    (
                        300011,
                        None,
                        1,
                        1,
                        100,
                        NEW_WATERMARK_TIMESTAMP,
                    ),
                    (
                        300012,
                        None,
                        1,
                        1,
                        200,
                        NEW_WATERMARK_TIMESTAMP,
                    ),
                ]
            ]
        ),
    )

    monkeypatch.setattr(
        incremental_load,
        "load_incremental_orders",
        lambda batches: (
            2,
            (
                NEW_WATERMARK_TIMESTAMP,
                300012,
            ),
        ),
    )

    def fake_quality_check():
        raise RuntimeError(
            "Simulated quality failure"
        )

    monkeypatch.setattr(
        incremental_load,
        "run_staging_quality_checks",
        fake_quality_check,
    )

    def fake_update_watermark(*args, **kwargs):
        watermark_updated["value"] = True

    monkeypatch.setattr(
        incremental_load,
        "update_watermark",
        fake_update_watermark,
    )

    def fake_fail_etl_run(
        run_id,
        error_message,
    ):
        failed["run_id"] = run_id
        failed["error_message"] = error_message

    monkeypatch.setattr(
        incremental_load,
        "fail_etl_run",
        fake_fail_etl_run,
    )

    with pytest.raises(
        RuntimeError,
        match="Simulated quality failure",
    ):
        incremental_load.run_incremental_load()

    assert failed["run_id"] == 102

    assert (
        failed["error_message"]
        == "Simulated quality failure"
    )

    assert watermark_updated["value"] is False



def test_composite_watermark_orders_by_timestamp_then_order_id():
    timestamp_a = datetime(
        2022,
        1,
        1,
        12,
        0,
        0,
        tzinfo=timezone.utc,
    )

    timestamp_b = datetime(
        2022,
        1,
        2,
        12,
        0,
        0,
        tzinfo=timezone.utc,
    )

    watermarks = [
        (timestamp_a, 300005),
        (timestamp_b, 100000),
        (timestamp_b, 150000),
    ]

    assert max(watermarks) == (
        timestamp_b,
        150000,
    )


def test_composite_watermark_tie_breaker_uses_order_id():
    timestamp = datetime(
        2022,
        1,
        2,
        12,
        0,
        0,
        tzinfo=timezone.utc,
    )

    current_watermark = (
        timestamp,
        300001,
    )

    candidate_watermark = (
        timestamp,
        300002,
    )

    assert candidate_watermark > current_watermark