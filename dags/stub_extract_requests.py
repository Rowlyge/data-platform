"""
Stub DAG: проверка связи Airflow -> proxy-postgres через Airflow Connection.
Подключается к таблице requests, читает несколько последних строк и пишет в лог.
Это временная заглушка перед полноценным инкрементальным extraction DAG-ом.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "proxy_postgres"


def read_sample_rows(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
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
    description="Stub: connect to proxy-postgres (via Airflow Connection) and log sample requests",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["stub", "extraction"],
) as dag:

    read_rows_task = PythonOperator(
        task_id="read_sample_rows",
        python_callable=read_sample_rows,
    )
