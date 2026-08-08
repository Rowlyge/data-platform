"""
Stub DAG: проверка связи Airflow -> MinIO через Airflow Connection (S3Hook).
Кладёт тестовый файл в bucket data-lake, читает его обратно и логирует содержимое.
Это временная заглушка перед полноценной записью Parquet в Raw layer.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

S3_CONN_ID = "minio_s3"
BUCKET_NAME = "data-lake"
TEST_KEY = "stub/roundtrip_test.txt"


def write_and_read_test_file(**context):
    hook = S3Hook(aws_conn_id=S3_CONN_ID)

    test_content = f"Hello from Airflow at {context['ts']}"

    hook.load_string(
        string_data=test_content,
        key=TEST_KEY,
        bucket_name=BUCKET_NAME,
        replace=True,
    )
    print(f"Written to s3://{BUCKET_NAME}/{TEST_KEY}: {test_content}")

    read_back = hook.read_key(key=TEST_KEY, bucket_name=BUCKET_NAME)
    print(f"Read back from s3://{BUCKET_NAME}/{TEST_KEY}: {read_back}")

    assert read_back == test_content, "Roundtrip mismatch!"
    print("Roundtrip check passed.")

    keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix="stub/")
    print(f"Keys under stub/: {keys}")


default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="stub_minio_roundtrip",
    description="Stub: write/read a test file to/from MinIO via Airflow Connection",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["stub", "minio"],
) as dag:

    roundtrip_task = PythonOperator(
        task_id="write_and_read_test_file",
        python_callable=write_and_read_test_file,
    )
