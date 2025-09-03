from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from airflow.providers.postgres.hooks.postgres import PostgresHook
from src_gen_mock import generate_mock

from airflow import DAG


def generate_data():
    return generate_mock.generate_product()

def insert_product(**kwargs):
    ti = kwargs['ti']
    product = ti.xcom_pull(task_ids = 'generate_data')
    if not product:
        raise ValueError('No product data received from the previous task')
    
    hook = PostgresHook(postgres_conn_id = "my_postgres")
    column_names = product.keys()
    values = [product[col] for col in column_names]
    insert_sql = f"""
        INSERT INTO products({", ".join(column_names)})
        VALUES({", ".join(["%s"] * len(column_names))})        
        """
    hook.run(insert_sql, parameters = values)
default_args = {
    'owner': 'delia',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'generate_products',
    default_args = default_args,
    description = 'generates products and places them in the postgres database',
    start_date = datetime(2025, 9, 1, 1),
    schedule='@hourly'
) as dag:
    task1 = PythonOperator(
        task_id = "generate_data",
        python_callable = generate_data
)
    task2 = PythonOperator(
        task_id="insert_product",
        python_callable=insert_product,

    )

task1 >>task2
