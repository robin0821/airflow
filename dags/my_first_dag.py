from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'robin d',
    'retries': 3, 
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'my_first_dag',
    description = 'This is my first DAG',
    default_args = default_args,
    start_date = datetime(2025, 3, 20, 21),
    schedule_interval = '@daily'
) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command = "echo hello world!"
    )