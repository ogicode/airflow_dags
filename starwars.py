from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import random
import requests
import logging

# Define the DAG
default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

# Define the DAG
with DAG(
    dag_id='starwars_character_dag',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='*/5 * * * *',  # Runs every 5 seconds
    catchup=False
) as dag:

    @task
    def generate_random_number():
        return random.randint(1, 83)

    @task
    def fetch_character(random_number: int):
        url = f'https://swapi.dev/api/people/{random_number}/'
        response = requests.get(url).json()
        character_name = response.get('name')
        logging.info(f"Fetched Character: {character_name}")  # Use logging instead of print
        return character_name

    @task
    def print_character(character_name: str):
        logging.info(f"Star Wars Character: {character_name}")  # Use logging to ensure it prints to Airflow logs

    # Task dependencies
    random_number = generate_random_number()
    character_name = fetch_character(random_number)
    print_character(character_name)
