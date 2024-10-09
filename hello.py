from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the Hello World function
def hello_world():
    print("Hello, World!")

# Define the DAG
with DAG(
    dag_id='hello_world_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Define a Python task
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=hello_world
    )
