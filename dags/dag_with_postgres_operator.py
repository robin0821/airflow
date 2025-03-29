from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'robin',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='dag_with_sql_operator_v01',
    default_args=default_args,
    start_date=datetime(2025, 1, 20),
    schedule='0 0 * * *'
) as dag:
    task1 = SQLExecuteQueryOperator(
        task_id='create_postgis_table',
        conn_id='venus',
        sql="""
            create table if not exists transformations.dag_runs (
            dt date, 
            dag_id character varying,
            primary key (dt, dag_id))
    """
    )

    task2 = SQLExecuteQueryOperator(
        task_id='delete_data_from_table',
        conn_id='venus',
        sql="""
            delete from transformations.dag_runs where dt = '{{ ds }}' and dag_id = '{{ dag.dag_id }}'
    """
    )

    task3 = SQLExecuteQueryOperator(
        task_id='insert_into_table',
        conn_id='venus',
        sql="""
            insert into transformations.dag_runs (dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id }}')
    """
    )

    task1 >> task2 >> task3
    
