from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import random
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from airflow.providers.postgres.hooks.postgres import PostgresHook
from src_gen_mock import generate_mock
from utils.get_random_entries import *
from airflow import DAG


def get_random_data():
    customer_id = get_random_customer()
    # choose a random number of order_items to generate
    number_of_items = random.randint(1, 3)
    order_items_info_list = []

    for i in range(1, number_of_items):
        product_id = get_random_product()
        seller_id = get_random_seller()
        order_item_info = (product_id, seller_id)
        order_items_info_list.append(order_item_info)

    return (customer_id, order_items_info_list)

def generate_order(**kwargs):
    ti = kwargs['ti']
    info = ti.xcom_pull(task_ids = 'get_random_customer')
    if not info:
        raise ValueError("No info received from the previous task")
    

    customer_id = info[0]
    order_items_info_list = info[1]


    random_order = generate_mock.generate_order(customer_id)
    column_names = random_order.keys()
    values = list(random_order.values())

    # first place the order, then take its id
    hook = PostgresHook(postgres_conn_id = "my_postgres")
    insert_order_hook = f"""
        INSERT INTO ORDERS({", ".join(column_names)})
        VALUES ({", ".join(["%s"] * len(column_names))})
        RETURNING order_id """
    order_id = hook.get_records(insert_order_hook, parameters = values)[0][0]

    # iterate through the order_items list and generate and insert order_items
    order_item_id = 1
    for order_item_info in order_items_info_list:
        order_item = generate_mock.generate_order_item(
            order_id, order_item_id, product_id=order_item_info[0], seller_id=order_item_info[1])
        
        column_names = order_item.keys()
        values = list(order_item.values())

        insert_item_order_hook = f"""
        INSERT INTO ORDER_ITEMS({", ".join(column_names)})
        VALUES ({", ".join(["%s"] * len(column_names))})"""
        hook.run(insert_item_order_hook, parameters = values)
        order_item_id += 1
    

default_args = {
    'owner': 'delia',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'generate_orders',
    default_args = default_args,
    description = 'generates orders and places them in the postgres database',
    start_date = datetime(2025, 9, 1, 1),
    schedule='*/2 * * * *'
) as dag:
    task1 = PythonOperator(
        task_id = "get_random_customer",
        python_callable = get_random_data
)
    task2 = PythonOperator(
        task_id="generate_and_insert_order",
        python_callable = generate_order,

)
task1 >> task2
