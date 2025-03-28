from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'robin d',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

def get_name(ti):
    ti.xcom_push(key='first_name', value='robin')
    ti.xcom_push(key='last_name', value='d')

def get_age(ti):
    ti.xcom_push(key='age', value='30')


def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    name = first_name + ", " + last_name 
    print(f"Hello World from Python Operator!"
          f"My name is {name}, and I am {age} years old!")

def sum(num1, num2):
    result = int(num1) + int(num2)
    print("The result of {num1} + {num2} is {result}".format(num1=num1, num2=num2, result=result))
    return result

with DAG(
    default_args=default_args,
    dag_id='my_dag_with_python_operator_v1',
    description='My first dag using python operator',
    start_date=datetime(2025, 3, 27),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet
        # op_kwargs={'age': 39}
    )
    task2 = PythonOperator(
        task_id='sum',
        python_callable=sum,
        op_kwargs={'num1': 3, 'num2': 5}
    )

    task3 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task4 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

    task3 >> task4 >> task1 >> task2