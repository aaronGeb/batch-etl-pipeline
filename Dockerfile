FROM apache/airflow:2.10.5

RUN python -m venv /home/airflow/dbt_venv \
    && /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
       dbt-core==1.11.11 dbt-postgres==1.10.0