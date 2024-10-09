from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='simple_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Define a single task
    start = DummyOperator(task_id='start')

