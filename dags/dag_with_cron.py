from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_arg = {
    'owner': 'robin',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)

}

with DAG(
    dag_id='dag_with_cron_expression_v01',
    default_args=default_arg,
    start_date=datetime(2025, 3, 18),
    schedule_interval='0 0 * * *', 
    catchup=True
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command="echo dag with cron expression"
    )