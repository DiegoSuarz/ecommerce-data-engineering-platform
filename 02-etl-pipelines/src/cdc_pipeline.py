from audit import (
    complete_cdc_batch,
    complete_etl_run,
    fail_etl_run,
    get_cdc_checkpoint,
    get_latest_cdc_batch_state,
    initialize_cdc_checkpoint_from_checkpoint,
    start_cdc_batch,
    start_etl_run,
)
from cdc_load import (
    get_loaded_batch_metrics,
    get_transformed_batch_metrics,
)
from logger import get_logger


logger = get_logger("cdc_pipeline")


PIPELINE_NAME = "change_data_capture"

APPLY_CHECKPOINT_NAME = (
    "mysql_sales_binlog"
)

READ_CHECKPOINT_NAME = (
    "mysql_sales_binlog_read"
)


def get_multistage_checkpoints():
    apply_checkpoint = (
        get_cdc_checkpoint(
            PIPELINE_NAME,
            APPLY_CHECKPOINT_NAME,
        )
    )

    if apply_checkpoint is None:
        raise RuntimeError(
            "CDC APPLY checkpoint does "
            "not exist."
        )

    read_checkpoint = (
        get_cdc_checkpoint(
            PIPELINE_NAME,
            READ_CHECKPOINT_NAME,
        )
    )

    if read_checkpoint is None:
        read_checkpoint = (
            initialize_cdc_checkpoint_from_checkpoint(
                PIPELINE_NAME,
                APPLY_CHECKPOINT_NAME,
                READ_CHECKPOINT_NAME,
            )
        )

    return {
        "apply": apply_checkpoint,
        "read": read_checkpoint,
    }


def assert_multistage_cdc_ready():
    checkpoints = (
        get_multistage_checkpoints()
    )

    if (
        checkpoints["read"]
        != checkpoints["apply"]
    ):
        raise RuntimeError(
            "CDC pipeline has pending staged "
            "work. READ and APPLY checkpoints "
            "must match before starting a new "
            "batch. "
            f"READ={checkpoints['read']} "
            f"APPLY={checkpoints['apply']}."
        )

    latest_batch = (
        get_latest_cdc_batch_state(
            PIPELINE_NAME
        )
    )

    if latest_batch is None:
        return checkpoints["apply"]

    if latest_batch["status"] != "SUCCESS":
        raise RuntimeError(
            "CDC pipeline has an incomplete "
            "CDC batch. A new batch cannot "
            "start until the previous run is "
            "completed successfully. "
            f"batch_id="
            f"{latest_batch['batch_id']} "
            f"run_id="
            f"{latest_batch['run_id']} "
            f"status="
            f"{latest_batch['status']}."
        )

    batch_end = (
        latest_batch[
            "end_binlog_file"
        ],
        latest_batch[
            "end_binlog_position"
        ],
    )

    if batch_end != checkpoints["apply"]:
        raise RuntimeError(
            "CDC pipeline audit/checkpoint "
            "mismatch. The latest successful "
            "CDC batch must end exactly at "
            "the APPLY checkpoint. "
            f"batch_id="
            f"{latest_batch['batch_id']} "
            f"batch_end={batch_end} "
            f"APPLY="
            f"{checkpoints['apply']}."
        )

    return checkpoints["apply"]


def start_multistage_cdc_run(
    orchestrator=None,
    orchestrator_run_id=None,
    orchestrator_task_id=None,
    orchestrator_try_number=None,
):
    start_coordinate = (
        assert_multistage_cdc_ready()
    )

    run_id = None

    try:
        run_id = start_etl_run(
            PIPELINE_NAME,
            orchestrator=orchestrator,
            orchestrator_run_id=(
                orchestrator_run_id
            ),
            orchestrator_task_id=(
                orchestrator_task_id
            ),
            orchestrator_try_number=(
                orchestrator_try_number
            ),
        )

        batch_id = start_cdc_batch(
            run_id=run_id,
            start_binlog_file=(
                start_coordinate[0]
            ),
            start_binlog_position=(
                start_coordinate[1]
            ),
        )

    except Exception as exc:
        if run_id is not None:
            fail_etl_run(
                run_id,
                str(exc),
            )

        raise

    result = {
        "run_id": run_id,
        "batch_id": batch_id,
        "start_binlog_file": (
            start_coordinate[0]
        ),
        "start_binlog_position": (
            start_coordinate[1]
        ),
    }

    logger.info(
        "Multi-stage CDC run started. "
        "run_id=%s batch_id=%s "
        "checkpoint=%s:%s",
        run_id,
        batch_id,
        start_coordinate[0],
        start_coordinate[1],
    )

    return result


def complete_multistage_cdc_run(
    run_id,
    batch_id,
    load_result,
):
    transformed_metrics = (
        get_transformed_batch_metrics(
            batch_id
        )
    )

    loaded_metrics = (
        get_loaded_batch_metrics(
            batch_id
        )
    )

    if (
        transformed_metrics["event_count"]
        != loaded_metrics["event_count"]
    ):
        raise RuntimeError(
            "CDC COMPLETE reconciliation "
            "failed: transformed_events="
            f"{transformed_metrics['event_count']} "
            "durable_final_events="
            f"{loaded_metrics['event_count']}."
        )

    complete_cdc_batch(
        batch_id=batch_id,
        end_binlog_file=(
            load_result[
                "end_binlog_file"
            ]
        ),
        end_binlog_position=(
            load_result[
                "end_binlog_position"
            ]
        ),
        transactions_processed=(
            transformed_metrics[
                "transaction_count"
            ]
        ),
        events_processed=(
            transformed_metrics[
                "event_count"
            ]
        ),
        insert_events=(
            transformed_metrics[
                "insert_count"
            ]
        ),
        update_events=(
            transformed_metrics[
                "update_count"
            ]
        ),
        delete_events=(
            transformed_metrics[
                "delete_count"
            ]
        ),
    )

    complete_etl_run(
        run_id,
        rows_extracted=(
            transformed_metrics[
                "event_count"
            ]
        ),
        rows_loaded=(
            loaded_metrics[
                "event_count"
            ]
        ),
        rows_rejected=0,
    )

    result = {
        "run_id": run_id,
        "batch_id": batch_id,
        **transformed_metrics,
        "events_inserted": (
            load_result[
                "events_inserted"
            ]
        ),
        "events_durable": (
            loaded_metrics[
                "event_count"
            ]
        ),
        "end_binlog_file": (
            load_result[
                "end_binlog_file"
            ]
        ),
        "end_binlog_position": (
            load_result[
                "end_binlog_position"
            ]
        ),
    }

    logger.info(
        "Multi-stage CDC run completed. "
        "run_id=%s batch_id=%s "
        "transactions=%s events=%s "
        "inserted_this_attempt=%s "
        "durable=%s checkpoint=%s:%s",
        run_id,
        batch_id,
        transformed_metrics[
            "transaction_count"
        ],
        transformed_metrics[
            "event_count"
        ],
        load_result[
            "events_inserted"
        ],
        loaded_metrics[
            "event_count"
        ],
        load_result[
            "end_binlog_file"
        ],
        load_result[
            "end_binlog_position"
        ],
    )

    return result


def fail_multistage_cdc_run(
    run_id,
    error_message,
):
    fail_etl_run(
        run_id,
        error_message,
    )

    logger.error(
        "Multi-stage CDC run failed. "
        "run_id=%s error=%s",
        run_id,
        error_message,
    )