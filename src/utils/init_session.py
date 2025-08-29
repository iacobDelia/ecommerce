from pyspark.sql import SparkSession
from etl.olist_db.transform import *;

def get_session():
    return SparkSession.builder \
    .appName("Olist ETL") \
    .config(
        "spark.jars.packages",
        "net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4,"
        "net.snowflake:snowflake-jdbc:3.24.2,"
        "org.postgresql:postgresql:42.2.5"
    ) \
    .getOrCreate()
