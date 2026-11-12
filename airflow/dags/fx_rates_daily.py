from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from fx_etl.extract import fetch_rates, flatten_rates
from fx_etl.load import upsert_rates

DBT = "/home/airflow/dbt_venv/bin/dbt"
DBT_ARGS = "--project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"


@dag(
    dag_id="fx_rates_daily",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["fx", "batch-etl"],
)
def fx_rates_daily():
    @task
    def extract(ds=None):
        return fetch_rates(ds)

    @task
    def load(payload, ds=None):
        rows = flatten_rates(payload)
        conn = PostgresHook("warehouse").get_conn()
        try:
            return upsert_rates(conn, rows, source_file=f"api:frankfurter:{ds}")
        finally:
            conn.close()

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT} run {DBT_ARGS}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test {DBT_ARGS}",
    )

    load(extract()) >> dbt_run >> dbt_test


fx_rates_daily()
