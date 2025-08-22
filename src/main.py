from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar") \
    .getOrCreate()
    
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://host.docker.internal:5432/postgres") \
    .option("dbtable", "cars") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.show()