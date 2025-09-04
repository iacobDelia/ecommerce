# OLIST data pipeline
**A data pipeline using PostgreSQL, PySpark and Snowflake, orchestrated by Airflow**

## Architecture
![Architecture pipeline](/etl_diagram.png)
The data is first injected inside PostgreSQL using CSV files taken from the public dataset released by OList. A few DAGs are also scheduled using Airflow to run periodically, inserting mock data generated using Python scripts.


Pyspark then processes the data. A few changes are done, such as transforming the database to resemble a snowflake schema, dropping unnecessary or duplicate columns, and computing the dimensions of the products into volume.
The data is then loaded to Snowflake.

Postgres, Airflow and Pyspark each have their own docker container. In the case of Pyspark, its container is started by Airflow.


## Project structure
```
ecommerce
├── dags                        --> DAGs used by Airflow 
│   ├── dag_gen_order.py
│   ├── dag_gen_products.py
│   ├── dag_pyspark.py
│   └── utils
│       └── get_random_entries.py
├── docker
│   └── Pyspark
│       └── Dockerfile
├── docker-compose.yml
├── README.md
├── src                         --> source code for Pyspark batch processing
│   ├── etl
│   │   └── olist_db
│   │       ├── extract.py
│   │       ├── load.py
│   │       └── transform.py
│   ├── main.py
│   └── utils
│       └── init_session.py
└── src_gen_mock                --> source code for generating mock data
    ├── generate_mock.py
    ├── __init__.py
    └── sample
        ├── geolocation_data.json
        └── product_category_names.json
```
## References
- Data used for initial ingestion - [OList public dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- docker-compose.yaml used as a base - [Official Airflow docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)