from logger import get_logger

logger = get_logger("full_load")

from audit import (
    complete_etl_run,
    fail_etl_run,
    start_etl_run,
)
from extract import (
    extract_categories,
    extract_countries,
    extract_orders,
)
from load import (
    load_categories,
    load_countries,
    load_dim_category,
    load_dim_country,
    load_dim_date,
    load_fact_sales,
    load_orders,
)
from quality import (
    reconcile_staging_to_dw,
    run_dw_quality_checks,
    run_staging_quality_checks,
)
from transform import (
    build_date_dimension_rows,
    extract_distinct_order_dates,
)


PIPELINE_NAME = "full_load"


def run_full_load():
    run_id = start_etl_run(PIPELINE_NAME)

    logger.info("Starting full load. run_id=%s", run_id)

    try:
        logger.info("Extracting categories and countries")

        categories = extract_categories()
        countries = extract_countries()

        logger.info(
            "Extracted categories=%s countries=%s",
            len(categories),
            len(countries),
        )

        logger.info("Loading staging tables")

        load_categories(categories)
        load_countries(countries)

        orders_loaded = load_orders(
            extract_orders(batch_size=10000)
        )

        logger.info(
            "Loaded staging orders=%s",
            orders_loaded,
        )

        logger.info("Running staging quality checks")
        run_staging_quality_checks()

        logger.info("Building date dimension")

        order_dates = extract_distinct_order_dates()
        date_rows = build_date_dimension_rows(order_dates)

        logger.info(
            "Generated date dimension rows=%s",
            len(date_rows),
        )

        logger.info("Loading dimensions")

        load_dim_date(date_rows)
        load_dim_category()
        load_dim_country()

        logger.info("Loading fact_sales")
        load_fact_sales()

        logger.info("Running DW quality checks")
        run_dw_quality_checks()

        logger.info("Running reconciliation")
        reconciliation = reconcile_staging_to_dw()

        rows_extracted = (
            len(categories)
            + len(countries)
            + orders_loaded
        )

        rows_loaded = (
            len(categories)
            + len(countries)
            + len(date_rows)
            + reconciliation["dw_rows"]
        )

        complete_etl_run(
            run_id=run_id,
            rows_extracted=rows_extracted,
            rows_loaded=rows_loaded,
            rows_rejected=0,
        )

        logger.info(
            "Full load completed successfully. "
            "run_id=%s extracted=%s loaded=%s",
            run_id,
            rows_extracted,
            rows_loaded,
        )

    except Exception as error:
        logger.exception(
            "Full load failed. run_id=%s",
            run_id,
        )

        fail_etl_run(
            run_id=run_id,
            error_message=str(error),
        )

        raise

if __name__ == "__main__":
    run_full_load()
