from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'robin d',
    'retries': 3, 
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'my_first_dag_v2',
    description = 'This is my first DAG',
    default_args = default_args,
    start_date = datetime(2025, 3, 20, 21),
    schedule_interval = '@daily'
) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command = "echo hello world!"
    )

    task2 = BashOperator(
        task_id = 'second_task',
        bash_command = "echo hey, I am task2 and will be running after task1"
    )

    task3 = BashOperator(
        task_id = 'third_task',
        bash_command = 'echo hey, I am task3 and will be running after task1'
    )

    # Task dependency method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # Task dependency method 2
    # task1 >> task2
    # task1 >> task3

    # Task dependency method 3
    task1 >> [task2, task3]