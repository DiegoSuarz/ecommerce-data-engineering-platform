from audit import (
    complete_etl_run,
    fail_etl_run,
    get_watermark,
    initialize_watermark,
    start_etl_run,
    update_watermark,
)
from extract import extract_incremental_orders
from load import (
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
)


PIPELINE_NAME = "incremental_load"
WATERMARK_NAME = "orders_order_id"

logger = get_logger(PIPELINE_NAME)


def run_incremental_load():
    run_id = start_etl_run(PIPELINE_NAME)

    logger.info(
        "Starting incremental load. run_id=%s",
        run_id,
    )

    try:
        current_watermark = get_watermark(
            PIPELINE_NAME,
            WATERMARK_NAME,
        )

        if current_watermark is None:
            current_watermark = initialize_watermark(
                PIPELINE_NAME,
                WATERMARK_NAME,
            )

            logger.info(
                "Watermark initialized automatically. value=%s",
                current_watermark,
            )

        logger.info(
            "Current watermark=%s",
            current_watermark,
        )

        loaded, new_watermark = load_incremental_orders(
            extract_incremental_orders(
                current_watermark,
                batch_size=10000,
            )
        )

        logger.info(
            "Incremental staging rows=%s",
            loaded,
        )

        if loaded == 0:
            complete_etl_run(
                run_id=run_id,
                rows_extracted=0,
                rows_loaded=0,
                rows_rejected=0,
            )

            logger.info(
                "No new data found. run_id=%s",
                run_id,
            )

            return

        run_staging_quality_checks()

        dates = extract_distinct_order_dates()
        date_rows = build_date_dimension_rows(dates)

        load_incremental_dim_date(date_rows)
        load_incremental_fact_sales()

        run_dw_quality_checks()

        reconciliation = reconcile_incremental_batch()

        update_watermark(
            PIPELINE_NAME,
            WATERMARK_NAME,
            new_watermark,
        )

        complete_etl_run(
            run_id=run_id,
            rows_extracted=loaded,
            rows_loaded=reconciliation["dw_rows"],
            rows_rejected=0,
        )

        logger.info(
            "Incremental load completed successfully. "
            "run_id=%s watermark=%s rows=%s",
            run_id,
            new_watermark,
            loaded,
        )

    except Exception as error:
        logger.exception(
            "Incremental load failed. run_id=%s",
            run_id,
        )

        fail_etl_run(
            run_id=run_id,
            error_message=str(error),
        )

        raise


if __name__ == "__main__":
    run_incremental_load()