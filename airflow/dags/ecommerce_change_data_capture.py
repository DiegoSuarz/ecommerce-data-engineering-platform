import os
import sys
from datetime import timedelta

import pendulum

from airflow.sdk import (
    dag,
    get_current_context,
    task,
)


ETL_PATH = "/opt/airflow/etl"

if ETL_PATH not in sys.path:
    sys.path.insert(
        0,
        ETL_PATH,
    )


from cdc_extract import (
    extract_cdc_batch,
)
from cdc_load import (
    load_transformed_cdc_batch,
)
from cdc_pipeline import (
    complete_multistage_cdc_run,
    fail_multistage_cdc_run,
    start_multistage_cdc_run,
)
from cdc_transform import (
    transform_cdc_batch,
)


LOCAL_TIMEZONE = pendulum.timezone(
    "America/Lima"
)

def configure_cdc_environment():
    from airflow.providers.mysql.hooks.mysql import (
        MySqlHook,
    )
    from airflow.providers.postgres.hooks.postgres import (
        PostgresHook,
    )

    mysql_hook = MySqlHook(
        mysql_conn_id="mysql_cdc_source"
    )

    postgres_hook = PostgresHook(
        postgres_conn_id="postgres_dw"
    )

    mysql_conn = mysql_hook.get_connection(
        "mysql_cdc_source"
    )

    postgres_conn = postgres_hook.get_connection(
        "postgres_dw"
    )

    os.environ.update(
        {
            "MYSQL_HOST": mysql_conn.host,
            "MYSQL_PORT": str(
                mysql_conn.port
            ),
            "MYSQL_DATABASE": (
                mysql_conn.schema
            ),
            "MYSQL_CDC_USER": (
                mysql_conn.login
            ),
            "MYSQL_CDC_PASSWORD": (
                mysql_conn.password
            ),
            "POSTGRES_HOST": (
                postgres_conn.host
            ),
            "POSTGRES_PORT": str(
                postgres_conn.port
            ),
            "POSTGRES_DATABASE": (
                postgres_conn.schema
            ),
            "POSTGRES_USER": (
                postgres_conn.login
            ),
            "POSTGRES_PASSWORD": (
                postgres_conn.password
            ),
        }
    )


@dag(
    dag_id="ecommerce_change_data_capture",
    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz=LOCAL_TIMEZONE,
    ),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=[
        "ecommerce",
        "cdc",
        "mysql",
    ],
)

def ecommerce_change_data_capture():

    @task(
        retries=0,
        execution_timeout=timedelta(
            minutes=5
        ),
    )
    def start_cdc():
        configure_cdc_environment()

        context = get_current_context()
        task_instance = context["ti"]

        result = start_multistage_cdc_run(
            orchestrator="airflow",
            orchestrator_run_id=(
                context["run_id"]
            ),
            orchestrator_task_id=(
                task_instance.task_id
            ),
            orchestrator_try_number=(
                task_instance.try_number
            ),
        )

        print(
            f"CDC run started: {result}"
        )

        return result


    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(
            minutes=5
        ),
    )
    def extract_cdc(start_result):
        configure_cdc_environment()

        result = extract_cdc_batch(
            batch_id=start_result[
                "batch_id"
            ],
        )

        print(
            f"CDC extract completed: {result}"
        )

        return result


    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(
            minutes=5
        ),
    )
    def transform_cdc(extract_result):
        configure_cdc_environment()

        result = transform_cdc_batch(
            batch_id=extract_result[
                "batch_id"
            ],
        )

        print(
            f"CDC transform completed: {result}"
        )

        return result


    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(
            minutes=5
        ),
    )
    def load_cdc(
        extract_result,
        transform_result,
    ):
        configure_cdc_environment()

        if (
            extract_result["batch_id"]
            != transform_result["batch_id"]
        ):
            raise RuntimeError(
                "CDC stage batch mismatch: "
                f"extract={extract_result['batch_id']} "
                f"transform={transform_result['batch_id']}."
            )

        result = (
            load_transformed_cdc_batch(
                batch_id=extract_result[
                    "batch_id"
                ],
                end_binlog_file=(
                    extract_result[
                        "end_binlog_file"
                    ]
                ),
                end_binlog_position=(
                    extract_result[
                        "end_binlog_position"
                    ]
                ),
            )
        )

        print(
            f"CDC load completed: {result}"
        )

        return result


    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(
            minutes=5
        ),
    )
    def complete_cdc(
        start_result,
        load_result,
    ):
        configure_cdc_environment()

        if (
            start_result["batch_id"]
            != load_result["batch_id"]
        ):
            raise RuntimeError(
                "CDC completion batch mismatch: "
                f"start={start_result['batch_id']} "
                f"load={load_result['batch_id']}."
            )

        result = (
            complete_multistage_cdc_run(
                run_id=start_result[
                    "run_id"
                ],
                batch_id=start_result[
                    "batch_id"
                ],
                load_result=load_result,
            )
        )

        print(
            f"CDC run completed: {result}"
        )

        return result

    @task(
        retries=0,
        trigger_rule="one_failed",
        execution_timeout=timedelta(
            minutes=5
        ),
    )
    def fail_cdc(start_result):
        configure_cdc_environment()

        error_message = (
            "One or more CDC pipeline stages "
            "failed after retries."
        )

        fail_multistage_cdc_run(
            run_id=start_result["run_id"],
            error_message=error_message,
        )

        # Deliberately fail this leaf task.
        # Otherwise Airflow could consider
        # the DagRun successful even though
        # an upstream CDC stage failed.
        raise RuntimeError(
            error_message
        )

    start_result = start_cdc()

    extract_result = extract_cdc(
        start_result
    )

    transform_result = transform_cdc(
        extract_result
    )

    load_result = load_cdc(
        extract_result,
        transform_result,
    )

    complete_result = complete_cdc(
        start_result,
        load_result,
    )

    failure_result = fail_cdc(
        start_result
    )


    (
        start_result
        >> extract_result
        >> transform_result
        >> load_result
        >> complete_result
    )

    extract_result >> failure_result
    transform_result >> failure_result
    load_result >> failure_result
    complete_result >> failure_result

ecommerce_change_data_capture()
