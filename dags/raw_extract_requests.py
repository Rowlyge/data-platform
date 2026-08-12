"""
Raw extraction DAG: инкрементально забирает requests за data_interval,
сохраняет как Parquet в MinIO (Raw layer), затем прогоняет dbt
(staging + marts models с тестами) и экспортирует marts в PostgreSQL
для стабильного доступа из Metabase.

Идемпотентность: повторный запуск за тот же data_interval перезаписывает
тот же Parquet-файл (replace=True) и полностью пересобирает marts-таблицы
(dbt table materialization + to_sql if_exists="replace") — никаких дублей.
"""
import io
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "proxy_postgres"
S3_CONN_ID = "minio_s3"
BUCKET_NAME = "data-lake"
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"


def extract_and_write_parquet(**context):
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]

    print(f"Data interval: [{data_interval_start} ; {data_interval_end})")

    # 1. Читаем данные за интервал из source Postgres
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, method, path, status_code, duration_ms,
                       response_size, upstream, client_ip, user_agent, created_at
                FROM requests
                WHERE created_at >= %s AND created_at < %s
                ORDER BY created_at;
                """,
                (data_interval_start, data_interval_end),
            )
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
    finally:
        conn.close()

    print(f"Rows matched in interval: {len(rows)}")

    # 2. Строим DataFrame (даже если пусто — пишем файл с нулевой строкой,
    #    чтобы поведение было предсказуемым и явным)
    df = pd.DataFrame(rows, columns=colnames)
    print(df)

    # 3. Конвертируем в Parquet в памяти (без временных файлов на диске)
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    # 4. Формируем ключ файла с партиционированием по дате интервала
    year = data_interval_start.strftime("%Y")
    month = data_interval_start.strftime("%m")
    day = data_interval_start.strftime("%d")
    s3_key = f"raw/requests/year={year}/month={month}/day={day}/data.parquet"

    # 5. Пишем в MinIO с перезаписью — гарантирует идемпотентность:
    #    повторный запуск за тот же интервал перезапишет тот же файл.
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    s3_hook.load_bytes(
        bytes_data=buffer.read(),
        key=s3_key,
        bucket_name=BUCKET_NAME,
        replace=True,
    )
    print(f"Written {len(df)} rows to s3://{BUCKET_NAME}/{s3_key}")


default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="raw_extract_requests",
    description="Extract requests, transform via dbt, and export marts to Postgres for Metabase",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 7, 29),
    catchup=False,
    tags=["extraction", "raw", "parquet", "dbt"],
) as dag:

    extract_and_write_task = PythonOperator(
        task_id="extract_and_write_parquet",
        python_callable=extract_and_write_parquet,
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
    )

    test_dbt = BashOperator(
        task_id="test_dbt",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
    )

    export_to_postgres = BashOperator(
        task_id="export_to_postgres",
        bash_command=f"cd {DBT_PROJECT_DIR} && python export_marts_to_postgres.py",
    )

    extract_and_write_task >> run_dbt >> test_dbt >> export_to_postgres
