from datetime import datetime, timedelta
import csv
import logging
from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


default_arg = {
    'owner': 'robin',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def postgres_to_s3(ds_nodash, ):
    # Step 1 - Query postgres table
    # Step 2 - Upload queries data to S3 bucket
    hook = PostgresHook(postgres_conn_id='venus')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from orders where date <= '20220501'")
    with NamedTemporaryFile(mode="w", suffix="get_order") as f:
    # with open("dags/get_orders.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(i[0] for i in cursor.description)
        csv_writer.writerows(cursor)
        f.flush()
        cursor.close()
        conn.close()
        logging.info(f"Saved orders data in {f.name}")
        s3_hook = S3Hook(aws_conn_id="minio_conn")
        s3_hook.load_file(
            filename=f.name,
            key=f.name,
            bucket_name="airflow",
            replace=True
        )

with DAG(
    dag_id='dag_with_postgres_hooks_v02',
    default_args=default_arg,
    start_date=datetime(2025, 4, 6),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id="postgres_to_s3",
        python_callable=postgres_to_s3
    )