from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_random_product():
    hook = PostgresHook(postgres_conn_id = "my_postgres")
    records = hook.get_records("""
                SELECT product_id
                FROM products
                ORDER BY RANDOM()
                LIMIT 1;""")
    if not records:
        raise ValueError("No products returned from the database")
    
    product_id = records[0][0]
    return product_id

def get_random_customer():
    hook = PostgresHook(postgres_conn_id = "my_postgres")
    records = hook.get_records("""
                SELECT customer_id
                FROM customers
                ORDER BY RANDOM()
                LIMIT 1;""")
    if not records:
        raise ValueError("No customers returned from the database")
    
    customer_id = records[0][0]
    return customer_id

def get_random_seller():
    hook = PostgresHook(postgres_conn_id = "my_postgres")
    records = hook.get_records("""
                SELECT seller_id
                FROM sellers
                ORDER BY RANDOM()
                LIMIT 1;""")
    if not records:
        raise ValueError("No customers returned from the database")
    
    seller_id = records[0][0]
    return seller_id