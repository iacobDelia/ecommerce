from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar") \
    .getOrCreate()
    
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/ecommerce_db") \
    .option("dbtable", "cars") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.show()
