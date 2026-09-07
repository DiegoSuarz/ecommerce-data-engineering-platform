import os
import sys
from datetime import timedelta
import pendulum
from airflow.sdk import dag, get_current_context, task

LOCAL_TIMEZONE = pendulum.timezone(
    "America/Lima"
)

ETL_PATH = "/opt/airflow/etl"

if ETL_PATH not in sys.path:
    sys.path.insert(0, ETL_PATH)


def configure_etl_environment():
    from airflow.providers.mysql.hooks.mysql import (
        MySqlHook,
    )
    from airflow.providers.postgres.hooks.postgres import (
        PostgresHook,
    )

    mysql_hook = MySqlHook(
        mysql_conn_id="mysql_source"
    )

    postgres_hook = PostgresHook(
        postgres_conn_id="postgres_dw"
    )

    mysql_conn = mysql_hook.get_connection(
        "mysql_source"
    )

    postgres_conn = postgres_hook.get_connection(
        "postgres_dw"
    )

    os.environ.update(
        {
            "MYSQL_HOST": mysql_conn.host,
            "MYSQL_PORT": str(mysql_conn.port),
            "MYSQL_DATABASE": mysql_conn.schema,
            "MYSQL_USER": mysql_conn.login,
            "MYSQL_PASSWORD": mysql_conn.password,
            "POSTGRES_HOST": postgres_conn.host,
            "POSTGRES_PORT": str(
                postgres_conn.port
            ),
            "POSTGRES_DATABASE": (
                postgres_conn.schema
            ),
            "POSTGRES_USER": postgres_conn.login,
            "POSTGRES_PASSWORD": (
                postgres_conn.password
            ),
        }
    )

@dag(
    dag_id="ecommerce_incremental_load",
    start_date=pendulum.datetime(
        2026,
        8,
        1,
        tz=LOCAL_TIMEZONE,
    ),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "etl", "incremental"],
)
def ecommerce_incremental_load():
    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=2),
    )

    def start_audit():
        configure_etl_environment()

        context = get_current_context()
        task_instance = context["task_instance"]

        from incremental_load import PIPELINE_NAME
        from audit import start_etl_run

        run_id = start_etl_run(
            PIPELINE_NAME,
            orchestrator="airflow",
            orchestrator_run_id=context["run_id"],
            orchestrator_task_id=task_instance.task_id,
            orchestrator_try_number=(
                task_instance.try_number
            ),
        )

        return run_id

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=2),
    )
    def get_current_watermark():
        configure_etl_environment()

        from incremental_load import (
            PIPELINE_NAME,
            WATERMARK_NAME,
        )
        from audit import (
            get_watermark,
            initialize_watermark,
        )

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

        return {
            "watermark_timestamp": (
                watermark_timestamp.isoformat()
            ),
            "watermark_order_id": (
                watermark_order_id
            ),
        }

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=5),
    )
    def stage_orders(watermark):
        configure_etl_environment()

        from datetime import datetime

        from incremental_load import (
            stage_incremental_orders,
        )

        watermark_timestamp = (
            datetime.fromisoformat(
                watermark["watermark_timestamp"]
            )
        )

        result = stage_incremental_orders(
            watermark_timestamp,
            watermark["watermark_order_id"],
        )

        if result["watermark_timestamp"] is not None:
            result["watermark_timestamp"] = (
                result[
                    "watermark_timestamp"
                ].isoformat()
            )

        return result

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=5),
    )
    def stage_category_snapshot():
        configure_etl_environment()

        from incremental_load import (
            stage_categories,
        )

        return stage_categories()

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=5),
    )
    def stage_country_snapshot():
        configure_etl_environment()

        from incremental_load import (
            stage_countries,
        )

        return stage_countries()

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=5),
    )
    def load_category_dimension_task():
        configure_etl_environment()

        from incremental_load import (
            load_category_dimension,
        )

        return load_category_dimension()

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=5),
    )
    def load_country_dimension_task():
        configure_etl_environment()

        from incremental_load import (
            load_country_dimension,
        )

        return load_country_dimension()

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=5),
    )
    def load_date_dimension_task():
        configure_etl_environment()

        from incremental_load import (
            load_incremental_date_dimension,
        )

        return load_incremental_date_dimension()

    @task(
        retries=1,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=3),
    )
    def validate_staging_task():
        configure_etl_environment()

        from incremental_load import (
            validate_incremental_staging,
        )

        return validate_incremental_staging()

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=5),
    )
    def load_fact_task():
        configure_etl_environment()

        from incremental_load import (
            load_incremental_fact,
        )

        return load_incremental_fact()

    @task(
        trigger_rule="none_failed_min_one_success",
        retries=1,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=3),
    )
    def validate_dw_task():
        configure_etl_environment()

        from incremental_load import (
            validate_incremental_dw,
        )

        return validate_incremental_dw()

    @task(
        retries=1,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=3),
    )
    def reconcile_load_task(stage_result):
        configure_etl_environment()

        if stage_result["rows_loaded"] == 0:
            return {
                "staging_rows": 0,
                "dw_rows": 0,
            }

        from incremental_load import (
            reconcile_incremental_load,
        )

        return reconcile_incremental_load()

    @task(
        retries=1,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=2),
    )
    def advance_watermark_task(stage_result):
        configure_etl_environment()

        from datetime import datetime

        from incremental_load import (
            advance_incremental_watermark,
        )

        watermark_timestamp = stage_result[
            "watermark_timestamp"
        ]

        watermark_order_id = stage_result[
            "watermark_order_id"
        ]

        if watermark_timestamp is None:
            return {
                "advanced": False,
            }

        advance_incremental_watermark(
            datetime.fromisoformat(
                watermark_timestamp
            ),
            watermark_order_id,
        )

        return {
            "advanced": True,
            "watermark_timestamp": (
                watermark_timestamp
            ),
            "watermark_order_id": (
                watermark_order_id
            ),
        }


    @task
    def empty_reconciliation():
        return {
            "staging_rows": 0,
            "dw_rows": 0,
        }

    @task(
        retries=1,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=2),
    )
    def finalize_success(
        audit_run_id,
        stage_result,
        reconciliation,
    ):
        configure_etl_environment()

        from audit import complete_etl_run

        complete_etl_run(
            audit_run_id,
            rows_extracted=stage_result[
                "rows_loaded"
            ],
            rows_loaded=reconciliation[
                "dw_rows"
            ],
            rows_rejected=0,
        )

        return {
            "run_id": audit_run_id,
            "status": "SUCCESS",
        }

    @task(
        trigger_rule="one_failed",
        retries=0,
        execution_timeout=timedelta(minutes=2),
    )
    def finalize_failure(audit_run_id):
        configure_etl_environment()

        from audit import fail_etl_run

        error_message = (
            "Airflow incremental pipeline failed. "
            "See Airflow task logs for the original error."
        )

        fail_etl_run(
            audit_run_id,
            error_message,
        )

        raise RuntimeError(error_message)

    @task.branch
    def choose_order_path(stage_result):
        if stage_result["rows_loaded"] > 0:
            return "load_date_dimension_task"

        return "empty_reconciliation"

    audit_run_id = start_audit()

    watermark = get_current_watermark()

    order_stage = stage_orders(
        watermark
    )

    category_stage = (
        stage_category_snapshot()
    )

    country_stage = (
        stage_country_snapshot()
    )

    staging_quality = (
        validate_staging_task()
    )

    category_dimension = (
        load_category_dimension_task()
    )

    country_dimension = (
        load_country_dimension_task()
    )

    order_path = choose_order_path(
        order_stage
    )

    date_dimension = (
        load_date_dimension_task()
    )

    zero_orders = (
        empty_reconciliation()
    )

    fact_load = load_fact_task()

    dw_quality = validate_dw_task()

    reconciliation = (
        reconcile_load_task(
            order_stage
        )
    )

    watermark_advance = (
        advance_watermark_task(
            order_stage
        )
    )

    success = finalize_success(
        audit_run_id,
        order_stage,
        reconciliation,
    )

    failure = finalize_failure(
        audit_run_id
    )


    audit_run_id >> watermark

    audit_run_id >> [
        category_stage,
        country_stage,
    ]

    [
        order_stage,
        category_stage,
        country_stage,
    ] >> staging_quality


    staging_quality >> [
        category_dimension,
        country_dimension,
    ]

    [
        category_dimension,
        country_dimension,
    ] >> order_path

    order_path >> [
        date_dimension,
        zero_orders,
    ]

    date_dimension >> fact_load

    [
        fact_load,
        zero_orders,
    ] >> dw_quality

    dw_quality >> reconciliation

    reconciliation >> watermark_advance

    watermark_advance >> success

    [
        watermark,
        order_stage,
        category_stage,
        country_stage,
        staging_quality,
        category_dimension,
        country_dimension,
        order_path,
        date_dimension,
        zero_orders,
        fact_load,
        dw_quality,
        reconciliation,
        watermark_advance,
        success,
    ] >> failure

ecommerce_incremental_load()
