import os
import sys
from datetime import timedelta

import pendulum

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


ETL_PATH = "/opt/airflow/etl"

if ETL_PATH not in sys.path:
    sys.path.insert(0, ETL_PATH)


LOCAL_TIMEZONE = pendulum.timezone(
    "America/Lima"
)


@dag(
    dag_id="ecommerce_audit_reconciliation",
    start_date=pendulum.datetime(
        2026,
        8,
        1,
        tz=LOCAL_TIMEZONE,
    ),
    schedule="30 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "audit", "maintenance"],
)
def ecommerce_audit_reconciliation():

    @task(
        retries=1,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=2),
    )
    def reconcile_stale_runs():
        postgres_hook = PostgresHook(
            postgres_conn_id="postgres_dw"
        )

        postgres_conn = postgres_hook.get_connection(
            "postgres_dw"
        )

        os.environ.update(
            {
                "POSTGRES_HOST": postgres_conn.host,
                "POSTGRES_PORT": str(postgres_conn.port),
                "POSTGRES_DATABASE": postgres_conn.schema,
                "POSTGRES_USER": postgres_conn.login,
                "POSTGRES_PASSWORD": postgres_conn.password,
            }
        )

        from audit import mark_stale_etl_runs

        stale_runs = mark_stale_etl_runs(
            pipeline_name="incremental_load",
            stale_after_minutes=15,
        )

        print(
            "Stale ETL runs reconciled:",
            stale_runs,
        )

        return stale_runs

    reconcile_stale_runs()


ecommerce_audit_reconciliation()
