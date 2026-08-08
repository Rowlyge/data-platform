"""
Incremental extraction DAG: логирует, сколько строк requests попадает
в текущий data_interval Airflow (schedule="@daily").

Идемпотентность обеспечивается самим Airflow: повторный запуск за тот же
интервал (data_interval_start/end) даёт тот же диапазон фильтрации.

Это шаг 2 из 3: логика фильтрации проверяется без записи в Parquet/MinIO.
Запись будет добавлена на следующем шаге.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "proxy_postgres"


def extract_requests_for_interval(**context):
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]

    print(f"Data interval: [{data_interval_start} ; {data_interval_end})")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, method, path, status_code, duration_ms, upstream, created_at
                FROM requests
                WHERE created_at >= %s AND created_at < %s
                ORDER BY created_at;
                """,
                (data_interval_start, data_interval_end),
            )
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

            print(f"Columns: {colnames}")
            print(f"Rows matched in interval: {len(rows)}")
            for row in rows:
                print(dict(zip(colnames, row)))
    finally:
        conn.close()


default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="incremental_extract_requests",
    description="Incrementally log requests matching each daily data_interval",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 7, 29),
    catchup=False,
    tags=["extraction", "incremental"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_requests_for_interval",
        python_callable=extract_requests_for_interval,
    )
