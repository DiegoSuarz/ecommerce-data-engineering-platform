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
        execution_timeout=timedelta(minutes=5),
    )
    def run_incremental_pipeline():
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        from airflow.providers.postgres.hooks.postgres import PostgresHook

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
                "POSTGRES_PORT": str(postgres_conn.port),
                "POSTGRES_DATABASE": postgres_conn.schema,
                "POSTGRES_USER": postgres_conn.login,
                "POSTGRES_PASSWORD": postgres_conn.password,
            }
        )

        context = get_current_context()
        task_instance = context["task_instance"]

        from incremental_load import run_incremental_load

        run_incremental_load(
            orchestrator="airflow",
            orchestrator_run_id=context["run_id"],
            orchestrator_task_id=task_instance.task_id,
            orchestrator_try_number=task_instance.try_number,
        )

    run_incremental_pipeline()

ecommerce_incremental_load()
