from unittest.mock import Mock

import pytest

import cdc_pipeline


def test_ready_when_read_equals_apply(
    monkeypatch,
):
    monkeypatch.setattr(
        cdc_pipeline,
        "get_cdc_checkpoint",
        lambda *args: (
            "binlog.000040",
            1200,
        ),
    )

    coordinate = (
        cdc_pipeline
        .assert_multistage_cdc_ready()
    )

    assert coordinate == (
        "binlog.000040",
        1200,
    )


def test_rejects_pending_staged_work(
    monkeypatch,
):
    def checkpoint(
        pipeline_name,
        checkpoint_name,
    ):
        if (
            checkpoint_name
            == cdc_pipeline
            .READ_CHECKPOINT_NAME
        ):
            return (
                "binlog.000041",
                500,
            )

        return (
            "binlog.000040",
            1200,
        )

    monkeypatch.setattr(
        cdc_pipeline,
        "get_cdc_checkpoint",
        checkpoint,
    )

    with pytest.raises(
        RuntimeError,
        match="pending staged work",
    ):
        (
            cdc_pipeline
            .assert_multistage_cdc_ready()
        )


def test_bootstraps_missing_read_checkpoint(
    monkeypatch,
):
    def checkpoint(
        pipeline_name,
        checkpoint_name,
    ):
        if (
            checkpoint_name
            == cdc_pipeline
            .READ_CHECKPOINT_NAME
        ):
            return None

        return (
            "binlog.000040",
            1200,
        )

    monkeypatch.setattr(
        cdc_pipeline,
        "get_cdc_checkpoint",
        checkpoint,
    )

    bootstrap = Mock(
        return_value=(
            "binlog.000040",
            1200,
        )
    )

    monkeypatch.setattr(
        cdc_pipeline,
        (
            "initialize_cdc_checkpoint_"
            "from_checkpoint"
        ),
        bootstrap,
    )

    coordinate = (
        cdc_pipeline
        .assert_multistage_cdc_ready()
    )

    assert coordinate == (
        "binlog.000040",
        1200,
    )

    bootstrap.assert_called_once_with(
        cdc_pipeline.PIPELINE_NAME,
        cdc_pipeline.APPLY_CHECKPOINT_NAME,
        cdc_pipeline.READ_CHECKPOINT_NAME,
    )


def test_start_multistage_run(
    monkeypatch,
):
    monkeypatch.setattr(
        cdc_pipeline,
        "assert_multistage_cdc_ready",
        lambda: (
            "binlog.000040",
            1200,
        ),
    )

    monkeypatch.setattr(
        cdc_pipeline,
        "start_etl_run",
        lambda *args, **kwargs: 500,
    )

    start_batch = Mock(
        return_value=600
    )

    monkeypatch.setattr(
        cdc_pipeline,
        "start_cdc_batch",
        start_batch,
    )

    result = (
        cdc_pipeline
        .start_multistage_cdc_run(
            orchestrator="airflow",
            orchestrator_run_id=(
                "scheduled__test"
            ),
            orchestrator_task_id=(
                "start_cdc"
            ),
            orchestrator_try_number=1,
        )
    )

    assert result["run_id"] == 500
    assert result["batch_id"] == 600

    start_batch.assert_called_once_with(
        run_id=500,
        start_binlog_file=(
            "binlog.000040"
        ),
        start_binlog_position=1200,
    )

def test_fail_multistage_cdc_run(
    monkeypatch,
):
    fail_run = Mock()

    monkeypatch.setattr(
        cdc_pipeline,
        "fail_etl_run",
        fail_run,
    )

    cdc_pipeline.fail_multistage_cdc_run(
        run_id=500,
        error_message=(
            "CDC stage failed after retries"
        ),
    )

    fail_run.assert_called_once_with(
        500,
        "CDC stage failed after retries",
    )

def test_complete_multistage_cdc_run(
    monkeypatch,
):
    metrics = {
        "transaction_count": 1,
        "event_count": 3,
        "insert_count": 1,
        "update_count": 1,
        "delete_count": 1,
    }

    monkeypatch.setattr(
        cdc_pipeline,
        "get_transformed_batch_metrics",
        lambda batch_id: metrics,
    )

    complete_batch = Mock()
    complete_run = Mock()

    monkeypatch.setattr(
        cdc_pipeline,
        "complete_cdc_batch",
        complete_batch,
    )

    monkeypatch.setattr(
        cdc_pipeline,
        "complete_etl_run",
        complete_run,
    )

    load_result = {
        "batch_id": 217,
        "raw_events": 3,
        "transformed_events": 3,
        "events_loaded": 3,
        "events_inserted": 3,
        "end_binlog_file": (
            "binlog.000032"
        ),
        "end_binlog_position": 3771,
    }

    result = (
        cdc_pipeline
        .complete_multistage_cdc_run(
            run_id=492,
            batch_id=217,
            load_result=load_result,
        )
    )

    complete_batch.assert_called_once_with(
        batch_id=217,
        end_binlog_file=(
            "binlog.000032"
        ),
        end_binlog_position=3771,
        transactions_processed=1,
        events_processed=3,
        insert_events=1,
        update_events=1,
        delete_events=1,
    )

    complete_run.assert_called_once_with(
        492,
        rows_extracted=3,
        rows_loaded=3,
        rows_rejected=0,
    )

    assert result[
        "transaction_count"
    ] == 1

    assert result[
        "event_count"
    ] == 3

    assert result[
        "events_inserted"
    ] == 3

    assert result[
        "end_binlog_file"
    ] == "binlog.000032"

    assert result[
        "end_binlog_position"
    ] == 3771
