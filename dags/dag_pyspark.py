from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'delia',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id = 'pyspark_dag',
    default_args = default_args,
    description = 'the pyspark dag to process ecommerce data',
    start_date = datetime(2025, 9, 1, 1),
    schedule='@hourly'
) as dag:
    task1 = DockerOperator(
        task_id="run_pyspark_job",
        image="ecommerce-pyspark:latest",
        api_version="auto",
        auto_remove='force',
        # command="python /app/main.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="ecommerce_default",
        mount_tmp_dir=False,
)
