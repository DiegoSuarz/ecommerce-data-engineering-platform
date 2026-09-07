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
        "stage_incremental_orders",
        lambda *args, **kwargs: {
            "rows_loaded": 0,
            "watermark_timestamp": None,
            "watermark_order_id": None,
        },
    )

    monkeypatch.setattr(
        incremental_load,
        "stage_categories",
        lambda: {"rows_loaded": 6},
    )

    monkeypatch.setattr(
        incremental_load,
        "stage_countries",
        lambda: {"rows_loaded": 56},
    )

    monkeypatch.setattr(
        incremental_load,
        "validate_incremental_staging",
        lambda: {},
    )

    monkeypatch.setattr(
        incremental_load,
        "load_category_dimension",
        lambda: None,
    )

    monkeypatch.setattr(
        incremental_load,
        "load_country_dimension",
        lambda: None,
    )

    monkeypatch.setattr(
        incremental_load,
        "validate_incremental_dw",
        lambda: {},
    )

    def fake_advance_watermark(*args, **kwargs):
        watermark_updated["value"] = True

    monkeypatch.setattr(
        incremental_load,
        "advance_incremental_watermark",
        fake_advance_watermark,
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
        "stage_incremental_orders",
        lambda *args, **kwargs: {
            "rows_loaded": 5,
            "watermark_timestamp": (
                NEW_WATERMARK_TIMESTAMP
            ),
            "watermark_order_id": 300015,
        },
    )

    monkeypatch.setattr(
        incremental_load,
        "stage_categories",
        lambda: {"rows_loaded": 6},
    )

    monkeypatch.setattr(
        incremental_load,
        "stage_countries",
        lambda: {"rows_loaded": 56},
    )

    monkeypatch.setattr(
        incremental_load,
        "validate_incremental_staging",
        lambda: {},
    )

    monkeypatch.setattr(
        incremental_load,
        "load_category_dimension",
        lambda: None,
    )

    monkeypatch.setattr(
        incremental_load,
        "load_country_dimension",
        lambda: None,
    )

    monkeypatch.setattr(
        incremental_load,
        "load_incremental_date_dimension",
        lambda: None,
    )

    monkeypatch.setattr(
        incremental_load,
        "load_incremental_fact",
        lambda: None,
    )

    monkeypatch.setattr(
        incremental_load,
        "validate_incremental_dw",
        lambda: {},
    )

    monkeypatch.setattr(
        incremental_load,
        "reconcile_incremental_load",
        lambda: {
            "staging_rows": 5,
            "dw_rows": 5,
        },
    )

    def fake_advance_watermark(
        watermark_timestamp,
        watermark_order_id,
    ):
        watermark_update["watermark_timestamp"] = (
            watermark_timestamp
        )
        watermark_update["watermark_order_id"] = (
            watermark_order_id
        )

    monkeypatch.setattr(
        incremental_load,
        "advance_incremental_watermark",
        fake_advance_watermark,
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
        "stage_incremental_orders",
        lambda *args, **kwargs: {
            "rows_loaded": 2,
            "watermark_timestamp": (
                NEW_WATERMARK_TIMESTAMP
            ),
            "watermark_order_id": 300012,
        },
    )

    monkeypatch.setattr(
        incremental_load,
        "stage_categories",
        lambda: {"rows_loaded": 6},
    )

    monkeypatch.setattr(
        incremental_load,
        "stage_countries",
        lambda: {"rows_loaded": 56},
    )

    def fake_quality_check():
        raise RuntimeError(
            "Simulated quality failure"
        )

    monkeypatch.setattr(
        incremental_load,
        "validate_incremental_staging",
        fake_quality_check,
    )

    def fake_advance_watermark(*args, **kwargs):
        watermark_updated["value"] = True

    monkeypatch.setattr(
        incremental_load,
        "advance_incremental_watermark",
        fake_advance_watermark,
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


def test_stage_incremental_orders_returns_metadata(
    monkeypatch,
):
    captured = {}

    def fake_extract_incremental_orders(
        watermark_timestamp,
        watermark_order_id,
        batch_size,
    ):
        captured["watermark_timestamp"] = (
            watermark_timestamp
        )
        captured["watermark_order_id"] = (
            watermark_order_id
        )
        captured["batch_size"] = batch_size

        return iter(
            [
                [
                    (
                        300011,
                        None,
                        1,
                        1,
                        100,
                        NEW_WATERMARK_TIMESTAMP,
                    )
                ]
            ]
        )

    def fake_load_incremental_orders(batches):
        captured["batches"] = list(batches)

        return (
            1,
            (
                NEW_WATERMARK_TIMESTAMP,
                300011,
            ),
        )

    monkeypatch.setattr(
        incremental_load,
        "extract_incremental_orders",
        fake_extract_incremental_orders,
    )

    monkeypatch.setattr(
        incremental_load,
        "load_incremental_orders",
        fake_load_incremental_orders,
    )

    result = (
        incremental_load.stage_incremental_orders(
            WATERMARK_TIMESTAMP,
            300010,
            batch_size=500,
        )
    )

    assert captured["watermark_timestamp"] == (
        WATERMARK_TIMESTAMP
    )
    assert captured["watermark_order_id"] == 300010
    assert captured["batch_size"] == 500

    assert result == {
        "rows_loaded": 1,
        "watermark_timestamp": (
            NEW_WATERMARK_TIMESTAMP
        ),
        "watermark_order_id": 300011,
    }


def test_stage_incremental_orders_no_new_data(
    monkeypatch,
):
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

    result = (
        incremental_load.stage_incremental_orders(
            WATERMARK_TIMESTAMP,
            300010,
        )
    )

    assert result == {
        "rows_loaded": 0,
        "watermark_timestamp": None,
        "watermark_order_id": None,
    }


def test_stage_categories(
    monkeypatch,
):
    source_rows = [
        (
            1,
            "ELEC",
            "Electronics",
            "Technology",
            NEW_WATERMARK_TIMESTAMP,
        ),
        (
            2,
            "HOME",
            "Home",
            "Household",
            NEW_WATERMARK_TIMESTAMP,
        ),
    ]

    captured = {}

    monkeypatch.setattr(
        incremental_load,
        "extract_categories",
        lambda: source_rows,
    )

    def fake_load_categories(rows):
        captured["rows"] = rows

    monkeypatch.setattr(
        incremental_load,
        "load_categories",
        fake_load_categories,
    )

    result = incremental_load.stage_categories()

    assert captured["rows"] == source_rows

    assert result == {
        "rows_loaded": 2,
    }


def test_stage_countries_transforms_before_load(
    monkeypatch,
):
    source_rows = [
        (
            32,
            "PE",
            "Peru",
            "LATAM",
            "STRATEGIC",
            NEW_WATERMARK_TIMESTAMP,
        )
    ]

    transformed_rows = [
        (
            32,
            "PE",
            "Peru",
            "LATAM",
            "STRATEGIC",
            NEW_WATERMARK_TIMESTAMP,
            "a" * 64,
        )
    ]

    captured = {}

    monkeypatch.setattr(
        incremental_load,
        "extract_countries",
        lambda: source_rows,
    )

    def fake_transform_country_rows(rows):
        captured["transform_input"] = rows

        return transformed_rows

    monkeypatch.setattr(
        incremental_load,
        "transform_country_rows",
        fake_transform_country_rows,
    )

    def fake_load_countries(rows):
        captured["load_input"] = rows

    monkeypatch.setattr(
        incremental_load,
        "load_countries",
        fake_load_countries,
    )

    result = incremental_load.stage_countries()

    assert captured["transform_input"] == (
        source_rows
    )

    assert captured["load_input"] == (
        transformed_rows
    )

    assert result == {
        "rows_loaded": 1,
    }


def test_load_incremental_date_dimension(
    monkeypatch,
):
    source_dates = [
        datetime(2022, 1, 1).date(),
        datetime(2022, 1, 2).date(),
    ]

    dimension_rows = [
        ("date-row-1",),
        ("date-row-2",),
    ]

    captured = {}

    monkeypatch.setattr(
        incremental_load,
        "extract_distinct_order_dates",
        lambda: source_dates,
    )

    def fake_build_date_dimension_rows(dates):
        captured["dates"] = dates
        return dimension_rows

    monkeypatch.setattr(
        incremental_load,
        "build_date_dimension_rows",
        fake_build_date_dimension_rows,
    )

    def fake_load_incremental_dim_date(rows):
        captured["rows"] = rows

    monkeypatch.setattr(
        incremental_load,
        "load_incremental_dim_date",
        fake_load_incremental_dim_date,
    )

    result = (
        incremental_load
        .load_incremental_date_dimension()
    )

    assert captured["dates"] == source_dates
    assert captured["rows"] == dimension_rows

    assert result == {
        "rows_processed": 2,
    }


def test_load_category_dimension(
    monkeypatch,
):
    called = {"value": False}

    def fake_load_dim_category():
        called["value"] = True

    monkeypatch.setattr(
        incremental_load,
        "load_dim_category",
        fake_load_dim_category,
    )

    incremental_load.load_category_dimension()

    assert called["value"] is True


def test_load_country_dimension(
    monkeypatch,
):
    called = {"value": False}

    def fake_load_dim_country():
        called["value"] = True

    monkeypatch.setattr(
        incremental_load,
        "load_dim_country",
        fake_load_dim_country,
    )

    incremental_load.load_country_dimension()

    assert called["value"] is True


def test_validate_incremental_staging(
    monkeypatch,
):
    expected = {
        "staging_orders": "ok",
        "staging_categories": "ok",
        "staging_countries": "ok",
    }

    monkeypatch.setattr(
        incremental_load,
        "run_staging_quality_checks",
        lambda: expected,
    )

    result = incremental_load.validate_incremental_staging()

    assert result == expected

def test_load_incremental_fact(
    monkeypatch,
):
    called = {"value": False}

    def fake_load_incremental_fact_sales():
        called["value"] = True

    monkeypatch.setattr(
        incremental_load,
        "load_incremental_fact_sales",
        fake_load_incremental_fact_sales,
    )

    incremental_load.load_incremental_fact()

    assert called["value"] is True


def test_validate_incremental_dw(
    monkeypatch,
):
    expected = {
        "fact_sales": "ok",
    }

    monkeypatch.setattr(
        incremental_load,
        "run_dw_quality_checks",
        lambda: expected,
    )

    result = (
        incremental_load
        .validate_incremental_dw()
    )

    assert result == expected


def test_reconcile_incremental_load(
    monkeypatch,
):
    expected = {
        "staging_rows": 5,
        "dw_rows": 5,
        "staging_amount": 1500,
        "dw_amount": 1500,
    }

    monkeypatch.setattr(
        incremental_load,
        "reconcile_incremental_batch",
        lambda: expected,
    )

    result = (
        incremental_load
        .reconcile_incremental_load()
    )

    assert result == expected


def test_advance_incremental_watermark(
    monkeypatch,
):
    captured = {}

    def fake_update_watermark(
        pipeline_name,
        watermark_name,
        watermark_timestamp,
        watermark_order_id,
    ):
        captured["pipeline_name"] = pipeline_name
        captured["watermark_name"] = watermark_name
        captured["watermark_timestamp"] = (
            watermark_timestamp
        )
        captured["watermark_order_id"] = (
            watermark_order_id
        )

    monkeypatch.setattr(
        incremental_load,
        "update_watermark",
        fake_update_watermark,
    )

    incremental_load.advance_incremental_watermark(
        NEW_WATERMARK_TIMESTAMP,
        300015,
    )

    assert captured == {
        "pipeline_name": (
            incremental_load.PIPELINE_NAME
        ),
        "watermark_name": (
            incremental_load.WATERMARK_NAME
        ),
        "watermark_timestamp": (
            NEW_WATERMARK_TIMESTAMP
        ),
        "watermark_order_id": 300015,
    }


def test_incremental_load_processes_dimensions_without_new_orders(
    monkeypatch,
):
    calls = {
    "stage_categories": 0,
    "stage_countries": 0,
    "validate_staging": 0,
    "load_category_dimension": 0,
    "load_country_dimension": 0,
    "load_date_dimension": 0,
    "load_fact": 0,
    "validate_dw": 0,
    "reconciliation": 0,
    "watermark": 0,
}

    monkeypatch.setattr(
        incremental_load,
        "start_etl_run",
        lambda pipeline_name, **kwargs: 200,
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
        "stage_incremental_orders",
        lambda *args, **kwargs: {
            "rows_loaded": 0,
            "watermark_timestamp": None,
            "watermark_order_id": None,
        },
    )

    def fake_stage_categories():
        calls["stage_categories"] += 1
        return {"rows_loaded": 6}

    def fake_stage_countries():
        calls["stage_countries"] += 1
        return {"rows_loaded": 56}

    monkeypatch.setattr(
        incremental_load,
        "stage_categories",
        fake_stage_categories,
    )

    monkeypatch.setattr(
        incremental_load,
        "stage_countries",
        fake_stage_countries,
    )

    def fake_validate_incremental_staging():
        calls["validate_staging"] += 1
        return {}


    monkeypatch.setattr(
            incremental_load,
            "validate_incremental_staging",
            fake_validate_incremental_staging,
        )

    def fake_load_category_dimension():
        calls["load_category_dimension"] += 1

    def fake_load_country_dimension():
        calls["load_country_dimension"] += 1

    monkeypatch.setattr(
        incremental_load,
        "load_category_dimension",
        fake_load_category_dimension,
    )

    monkeypatch.setattr(
        incremental_load,
        "load_country_dimension",
        fake_load_country_dimension,
    )

    def fake_load_date_dimension():
        calls["load_date_dimension"] += 1

    def fake_load_fact():
        calls["load_fact"] += 1

    monkeypatch.setattr(
        incremental_load,
        "load_incremental_date_dimension",
        fake_load_date_dimension,
    )

    monkeypatch.setattr(
        incremental_load,
        "load_incremental_fact",
        fake_load_fact,
    )

    def fake_validate_incremental_dw():
        calls["validate_dw"] += 1
        return {}

    monkeypatch.setattr(
        incremental_load,
        "validate_incremental_dw",
        fake_validate_incremental_dw,
    )

    def fake_reconcile_incremental_load():
        calls["reconciliation"] += 1
        return {
            "staging_rows": 0,
            "dw_rows": 0,
        }


    monkeypatch.setattr(
        incremental_load,
        "reconcile_incremental_load",
        fake_reconcile_incremental_load,
    )

    def fake_advance_watermark(*args, **kwargs):
        calls["watermark"] += 1

    monkeypatch.setattr(
        incremental_load,
        "advance_incremental_watermark",
        fake_advance_watermark,
    )

    monkeypatch.setattr(
        incremental_load,
        "complete_etl_run",
        lambda *args, **kwargs: None,
    )

    incremental_load.run_incremental_load()

    assert calls["stage_categories"] == 1
    assert calls["stage_countries"] == 1

    assert calls["validate_staging"] == 1

    assert calls["load_category_dimension"] == 1
    assert calls["load_country_dimension"] == 1

    assert calls["load_date_dimension"] == 0
    assert calls["load_fact"] == 0
    assert calls["validate_dw"] == 1
    assert calls["reconciliation"] == 0
    assert calls["watermark"] == 0


def test_incremental_load_marks_audit_failed(
    monkeypatch,
):
    failed = {}

    monkeypatch.setattr(
        incremental_load,
        "start_etl_run",
        lambda pipeline_name, **kwargs: 201,
    )

    monkeypatch.setattr(
        incremental_load,
        "get_watermark",
        lambda pipeline_name, watermark_name: (
            WATERMARK_TIMESTAMP,
            300010,
        ),
    )

    def fake_stage_incremental_orders(*args, **kwargs):
        raise RuntimeError(
            "Controlled pipeline failure"
        )

    monkeypatch.setattr(
        incremental_load,
        "stage_incremental_orders",
        fake_stage_incremental_orders,
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
        match="Controlled pipeline failure",
    ):
        incremental_load.run_incremental_load()

    assert failed == {
        "run_id": 201,
        "error_message": (
            "Controlled pipeline failure"
        ),
    }
