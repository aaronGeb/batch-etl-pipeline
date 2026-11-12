from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from fx_etl.extract import fetch_rates, flatten_rates
from fx_etl.load import upsert_rates


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

    load(extract())


fx_rates_daily()
