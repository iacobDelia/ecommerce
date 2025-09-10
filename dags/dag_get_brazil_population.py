from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import random
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from src_gen_mock import generate_mock
from utils.get_brazil_population import *
from airflow import DAG


def insert_into_snowflake(**kwargs):
    ti = kwargs['ti']
    population_data = ti.xcom_pull(task_ids = 'retrieve_population_data')
    if not population_data:
        raise ValueError("No info received from the previous task")
    
    hook = SnowflakeHook(snowflake_conn_id="snowflake")
    conn = hook.get_conn()
    cur = conn.cursor()

    # create table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ibge_population (
            state VARCHAR,
            population INT
        )
    """)

    for row in population_data:
        cur.execute(
            """
            INSERT INTO ibge_population (state, population)
            VALUES (%s, %s)
            """,
            (row["State"], row["Population"])
        )

    conn.commit()
    cur.close()
    conn.close()
    print("Insert complete!")

default_args = {
    'owner': 'delia',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'get_brazil_population',
    default_args = default_args,
    description = 'retrieves a table with info about brazils population for each state and puts it in the snowflake db',
    start_date = datetime(2025, 9, 1, 1),
    schedule= '@yearly'
) as dag:
    task1 = PythonOperator(
        task_id = "retrieve_population_data",
        python_callable = get_population_dict
)
    task2 = PythonOperator(
        task_id="insert_into_snowflake",
        python_callable = insert_into_snowflake,
)
task1 >> task2
