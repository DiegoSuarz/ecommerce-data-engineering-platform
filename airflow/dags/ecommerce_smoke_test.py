from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="ecommerce_smoke_test",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "smoke-test"],
)
def ecommerce_smoke_test():

    @task
    def airflow_is_running():
        message = "Airflow orchestration is working."
        print(message)
        return message

    airflow_is_running()


ecommerce_smoke_test()
