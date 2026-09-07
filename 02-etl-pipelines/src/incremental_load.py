from audit import (
    complete_etl_run,
    fail_etl_run,
    get_watermark,
    initialize_watermark,
    start_etl_run,
    update_watermark,
)

from extract import (
    extract_categories,
    extract_countries,
    extract_incremental_orders,
)

from load import (
    load_categories,
    load_countries,
    load_dim_category,
    load_dim_country,
    load_incremental_dim_date,
    load_incremental_fact_sales,
    load_incremental_orders,
)

from logger import get_logger

from quality import (
    reconcile_incremental_batch,
    run_dw_quality_checks,
    run_staging_quality_checks,
)

from transform import (
    build_date_dimension_rows,
    extract_distinct_order_dates,
    transform_country_rows,
)


PIPELINE_NAME = "incremental_load"
WATERMARK_NAME = "orders_updated_at_order_id"


logger = get_logger(PIPELINE_NAME)

def stage_incremental_orders(
    watermark_timestamp,
    watermark_order_id,
    batch_size=10000,
):
    orders_batches = extract_incremental_orders(
        watermark_timestamp,
        watermark_order_id,
        batch_size=batch_size,
    )

    rows_loaded, new_watermark = load_incremental_orders(
        orders_batches
    )

    if new_watermark is None:
        new_watermark_timestamp = None
        new_watermark_order_id = None
    else:
        (
            new_watermark_timestamp,
            new_watermark_order_id,
        ) = new_watermark

    logger.info(
        "Incremental orders staged. rows=%s",
        rows_loaded,
    )

    return {
        "rows_loaded": rows_loaded,
        "watermark_timestamp": new_watermark_timestamp,
        "watermark_order_id": new_watermark_order_id,
    }

def stage_categories():
    rows = extract_categories()

    load_categories(rows)

    rows_loaded = len(rows)

    logger.info(
        "Categories staged. rows=%s",
        rows_loaded,
    )

    return {
        "rows_loaded": rows_loaded,
    }

def stage_countries():
    rows = extract_countries()

    transformed_rows = transform_country_rows(
        rows
    )

    load_countries(
        transformed_rows
    )

    rows_loaded = len(transformed_rows)

    logger.info(
        "Countries staged. rows=%s",
        rows_loaded,
    )

    return {
        "rows_loaded": rows_loaded,
    }

def load_incremental_date_dimension():
    dates = extract_distinct_order_dates()

    date_rows = build_date_dimension_rows(
        dates
    )

    load_incremental_dim_date(
        date_rows
    )

    rows_processed = len(date_rows)

    logger.info(
        "Incremental date dimension processed. rows=%s",
        rows_processed,
    )

    return {
        "rows_processed": rows_processed,
    }

def load_category_dimension():
    load_dim_category()

    logger.info(
        "Category dimension processed successfully."
    )

def load_country_dimension():
    load_dim_country()

    logger.info(
        "Country dimension processed successfully."
    )

def validate_incremental_staging():
    result = run_staging_quality_checks()

    logger.info(
        "Incremental staging quality checks passed."
    )

    return result

def load_incremental_fact():
    load_incremental_fact_sales()

    logger.info(
        "Incremental fact_sales processed successfully."
    )

def validate_incremental_dw():
    result = run_dw_quality_checks()

    logger.info(
        "Incremental DW quality checks passed."
    )

    return result

def reconcile_incremental_load():
    reconciliation = (
        reconcile_incremental_batch()
    )

    logger.info(
        "Incremental reconciliation completed. "
        "staging_rows=%s dw_rows=%s",
        reconciliation["staging_rows"],
        reconciliation["dw_rows"],
    )

    return reconciliation

def advance_incremental_watermark(
    watermark_timestamp,
    watermark_order_id,
):
    update_watermark(
        PIPELINE_NAME,
        WATERMARK_NAME,
        watermark_timestamp,
        watermark_order_id,
    )

    logger.info(
        "Composite watermark advanced. "
        "timestamp=%s order_id=%s",
        watermark_timestamp,
        watermark_order_id,
    )

def run_incremental_load(
    orchestrator=None,
    orchestrator_run_id=None,
    orchestrator_task_id=None,
    orchestrator_try_number=None,
):
    run_id = start_etl_run(
        PIPELINE_NAME,
        orchestrator=orchestrator,
        orchestrator_run_id=orchestrator_run_id,
        orchestrator_task_id=orchestrator_task_id,
        orchestrator_try_number=(
            orchestrator_try_number
        ),
    )

    try:
        watermark = get_watermark(
            PIPELINE_NAME,
            WATERMARK_NAME,
        )

        if watermark is None:
            watermark = initialize_watermark(
                PIPELINE_NAME,
                WATERMARK_NAME,
            )

        (
            watermark_timestamp,
            watermark_order_id,
        ) = watermark

        order_stage = stage_incremental_orders(
            watermark_timestamp,
            watermark_order_id,
        )

        stage_categories()
        stage_countries()

        validate_incremental_staging()

        load_category_dimension()
        load_country_dimension()

        rows_loaded = order_stage["rows_loaded"]

        reconciliation = {
            "staging_rows": 0,
            "dw_rows": 0,
        }

        if rows_loaded > 0:
            load_incremental_date_dimension()

            load_incremental_fact()

        validate_incremental_dw()

        if rows_loaded > 0:
            reconciliation = (
                reconcile_incremental_load()
            )

            advance_incremental_watermark(
                order_stage[
                    "watermark_timestamp"
                ],
                order_stage[
                    "watermark_order_id"
                ],
            )

        complete_etl_run(
            run_id,
            rows_extracted=rows_loaded,
            rows_loaded=(
                reconciliation["dw_rows"]
            ),
            rows_rejected=0,
        )

        logger.info(
            "Incremental load completed successfully."
        )

    except Exception as exc:
        fail_etl_run(
            run_id,
            str(exc),
        )

        logger.exception(
            "Incremental load failed."
        )

        raise

if __name__ == "__main__":
    run_incremental_load()
