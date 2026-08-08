"""
Stub DAG: проверка связи Airflow -> proxy-postgres.
Подключается к таблице requests, читает несколько последних строк и пишет в лог.
Это временная заглушка перед полноценным инкрементальным extraction DAG-ом.
"""
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


def read_sample_rows(**context):
    conn = psycopg2.connect(
        host="proxy-postgres",
        port=5432,
        dbname="proxydb",
        user="proxy",
        password="proxy",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, method, path, status_code, duration_ms, upstream, created_at
                FROM requests
                ORDER BY created_at DESC
                LIMIT 5;
                """
            )
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

            print(f"Columns: {colnames}")
            for row in rows:
                print(dict(zip(colnames, row)))

            cur.execute("SELECT count(*) FROM requests;")
            total = cur.fetchone()[0]
            print(f"Total rows in requests: {total}")
    finally:
        conn.close()


default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="stub_extract_requests",
    description="Stub: connect to proxy-postgres and log sample requests",
    default_args=default_args,
    schedule=None,  # запускаем вручную, пока это заглушка
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["stub", "extraction"],
) as dag:

    read_rows_task = PythonOperator(
        task_id="read_sample_rows",
        python_callable=read_sample_rows,
    )
