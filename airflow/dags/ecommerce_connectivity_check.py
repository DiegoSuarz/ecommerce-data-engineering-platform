from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="ecommerce_connectivity_check",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "connectivity"],
)
def ecommerce_connectivity_check():

    @task
    def check_mysql():
        from airflow.providers.mysql.hooks.mysql import MySqlHook

        hook = MySqlHook(
            mysql_conn_id="mysql_source"
        )

        result = hook.get_first(
            """
            SELECT
                DATABASE(),
                COUNT(*)
            FROM orders;
            """
        )

        print(
            f"MySQL database={result[0]} "
            f"orders={result[1]}"
        )

    @task
    def check_postgres():
        from airflow.providers.postgres.hooks.postgres import (
            PostgresHook,
        )

        hook = PostgresHook(
            postgres_conn_id="postgres_dw"
        )

        result = hook.get_first(
            """
            SELECT
                current_database(),
                COUNT(*)
            FROM dw.fact_sales;
            """
        )

        print(
            f"PostgreSQL database={result[0]} "
            f"fact_sales={result[1]}"
        )

    check_mysql()
    check_postgres()


ecommerce_connectivity_check()
