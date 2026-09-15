from unittest.mock import Mock

import pytest

from cdc import pipeline as cdc_pipeline


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

    monkeypatch.setattr(
        cdc_pipeline,
        "get_latest_cdc_batch_state",
        lambda pipeline_name: None,
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

    monkeypatch.setattr(
        cdc_pipeline,
        "get_latest_cdc_batch_state",
        lambda pipeline_name: None,
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

    monkeypatch.setattr(
        cdc_pipeline,
        "get_loaded_batch_metrics",
        lambda batch_id: {
            "event_count": 3,
            "insert_count": 1,
            "update_count": 1,
            "delete_count": 1,
        },
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


def test_complete_uses_durable_loaded_count_after_idempotent_retry(
    monkeypatch,
):
    transformed_metrics = {
        "transaction_count": 1,
        "event_count": 3,
        "insert_count": 1,
        "update_count": 1,
        "delete_count": 1,
    }

    durable_metrics = {
        "event_count": 3,
        "insert_count": 1,
        "update_count": 1,
        "delete_count": 1,
    }

    monkeypatch.setattr(
        cdc_pipeline,
        "get_transformed_batch_metrics",
        lambda batch_id: (
            transformed_metrics
        ),
    )

    monkeypatch.setattr(
        cdc_pipeline,
        "get_loaded_batch_metrics",
        lambda batch_id: durable_metrics,
        raising=False,
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

        # Retry inserted nothing new.
        "events_inserted": 0,

        "events_applied": 0,
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

    complete_run.assert_called_once_with(
        492,
        rows_extracted=3,
        rows_loaded=3,
        rows_rejected=0,
    )

    assert result[
        "events_inserted"
    ] == 0

    assert result[
        "events_durable"
    ] == 3


def test_complete_rejects_transformed_final_count_mismatch(
    monkeypatch,
):
    transformed_metrics = {
        "transaction_count": 1,
        "event_count": 3,
        "insert_count": 1,
        "update_count": 1,
        "delete_count": 1,
    }

    durable_metrics = {
        "event_count": 2,
        "insert_count": 1,
        "update_count": 1,
        "delete_count": 0,
    }

    monkeypatch.setattr(
        cdc_pipeline,
        "get_transformed_batch_metrics",
        lambda batch_id: transformed_metrics,
    )

    monkeypatch.setattr(
        cdc_pipeline,
        "get_loaded_batch_metrics",
        lambda batch_id: durable_metrics,
        raising=False,
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
        "events_inserted": 0,
        "end_binlog_file": "binlog.000032",
        "end_binlog_position": 3771,
    }

    with pytest.raises(
        RuntimeError,
        match="reconciliation",
    ):
        cdc_pipeline.complete_multistage_cdc_run(
            run_id=492,
            batch_id=217,
            load_result=load_result,
        )

    complete_batch.assert_not_called()
    complete_run.assert_not_called()


@pytest.mark.parametrize(
    "status",
    [
        "RUNNING",
        "FAILED",
    ],
)
def test_ready_rejects_incomplete_previous_batch_when_checkpoints_match(
    monkeypatch,
    status,
):
    monkeypatch.setattr(
        cdc_pipeline,
        "get_cdc_checkpoint",
        lambda *args: (
            "binlog.000040",
            1200,
        ),
    )

    monkeypatch.setattr(
        cdc_pipeline,
        "get_latest_cdc_batch_state",
        lambda pipeline_name: {
            "batch_id": 217,
            "run_id": 492,
            "status": status,
            "start_binlog_file": (
                "binlog.000039"
            ),
            "start_binlog_position": 900,
            "end_binlog_file": (
                "binlog.000040"
            ),
            "end_binlog_position": 1200,
        },
        raising=False,
    )

    with pytest.raises(
        RuntimeError,
        match="incomplete CDC batch",
    ):
        (
            cdc_pipeline
            .assert_multistage_cdc_ready()
        )


def test_ready_rejects_completed_batch_audit_checkpoint_mismatch(
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

    monkeypatch.setattr(
        cdc_pipeline,
        "get_latest_cdc_batch_state",
        lambda pipeline_name: {
            "batch_id": 217,
            "run_id": 492,
            "status": "SUCCESS",
            "start_binlog_file": (
                "binlog.000039"
            ),
            "start_binlog_position": 900,
            "end_binlog_file": (
                "binlog.000039"
            ),
            "end_binlog_position": 1100,
        },
        raising=False,
    )

    with pytest.raises(
        RuntimeError,
        match="audit/checkpoint mismatch",
    ):
        (
            cdc_pipeline
            .assert_multistage_cdc_ready()
        )


def test_ready_accepts_completed_batch_matching_apply(
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

    monkeypatch.setattr(
        cdc_pipeline,
        "get_latest_cdc_batch_state",
        lambda pipeline_name: {
            "batch_id": 217,
            "run_id": 492,
            "status": "SUCCESS",
            "start_binlog_file": (
                "binlog.000039"
            ),
            "start_binlog_position": 900,
            "end_binlog_file": (
                "binlog.000040"
            ),
            "end_binlog_position": 1200,
        },
        raising=False,
    )

    coordinate = (
        cdc_pipeline
        .assert_multistage_cdc_ready()
    )

    assert coordinate == (
        "binlog.000040",
        1200,
    )


def test_bootstraps_fresh_apply_and_read(
    monkeypatch,
):
    checkpoints = {}

    def get_checkpoint(
        pipeline_name,
        checkpoint_name,
    ):
        return checkpoints.get(
            checkpoint_name
        )

    def initialize_checkpoint(
        pipeline_name,
        checkpoint_name,
        binlog_file,
        binlog_position,
    ):
        checkpoints.setdefault(
            checkpoint_name,
            (
                binlog_file,
                binlog_position,
            ),
        )

        return checkpoints[
            checkpoint_name
        ]

    def initialize_from_checkpoint(
        pipeline_name,
        source_checkpoint_name,
        target_checkpoint_name,
    ):
        checkpoints.setdefault(
            target_checkpoint_name,
            checkpoints[
                source_checkpoint_name
            ],
        )

        return checkpoints[
            target_checkpoint_name
        ]

    monkeypatch.setattr(
        cdc_pipeline,
        "get_cdc_checkpoint",
        get_checkpoint,
    )

    monkeypatch.setattr(
        cdc_pipeline,
        "initialize_cdc_checkpoint",
        initialize_checkpoint,
    )

    monkeypatch.setattr(
        cdc_pipeline,
        (
            "initialize_cdc_checkpoint_"
            "from_checkpoint"
        ),
        initialize_from_checkpoint,
    )

    monkeypatch.setattr(
        cdc_pipeline,
        "get_latest_cdc_batch_state",
        lambda pipeline_name: None,
    )

    head = Mock(
        return_value=(
            "binlog.000042",
            157,
        )
    )

    monkeypatch.setattr(
        cdc_pipeline,
        "get_current_binlog_coordinate",
        head,
    )

    coordinate = (
        cdc_pipeline
        .assert_multistage_cdc_ready()
    )

    assert coordinate == (
        "binlog.000042",
        157,
    )

    assert checkpoints[
        cdc_pipeline.APPLY_CHECKPOINT_NAME
    ] == coordinate

    assert checkpoints[
        cdc_pipeline.READ_CHECKPOINT_NAME
    ] == coordinate

    head.assert_called_once_with()


def test_rejects_missing_apply_with_existing_read(
    monkeypatch,
):
    def get_checkpoint(
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
                900,
            )

        return None

    monkeypatch.setattr(
        cdc_pipeline,
        "get_cdc_checkpoint",
        get_checkpoint,
    )

    monkeypatch.setattr(
        cdc_pipeline,
        "get_latest_cdc_batch_state",
        lambda pipeline_name: None,
    )

    head = Mock()

    monkeypatch.setattr(
        cdc_pipeline,
        "get_current_binlog_coordinate",
        head,
    )

    with pytest.raises(
        RuntimeError,
        match="fresh CDC state",
    ):
        (
            cdc_pipeline
            .assert_multistage_cdc_ready()
        )

    head.assert_not_called()


def test_rejects_missing_apply_with_batch_history(
    monkeypatch,
):
    monkeypatch.setattr(
        cdc_pipeline,
        "get_cdc_checkpoint",
        lambda *args: None,
    )

    monkeypatch.setattr(
        cdc_pipeline,
        "get_latest_cdc_batch_state",
        lambda pipeline_name: {
            "batch_id": 7,
            "run_id": 9,
            "status": "SUCCESS",
            "start_binlog_file":
                "binlog.000040",
            "start_binlog_position": 700,
            "end_binlog_file":
                "binlog.000041",
            "end_binlog_position": 900,
        },
    )

    head = Mock()

    monkeypatch.setattr(
        cdc_pipeline,
        "get_current_binlog_coordinate",
        head,
    )

    with pytest.raises(
        RuntimeError,
        match="fresh CDC state",
    ):
        (
            cdc_pipeline
            .assert_multistage_cdc_ready()
        )

    head.assert_not_called()
