
def get_df_products(spark):
    return spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres_ecommerce:5432/ecommerce_db") \
    .option("dbtable", "products") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .option("driver", "org.postgresql.Driver") \
    .load()

def get_df_sellers(spark):
    return spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres_ecommerce:5432/ecommerce_db") \
    .option("dbtable", "sellers") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .option("driver", "org.postgresql.Driver") \
    .load()

def get_df_customers(spark):
    return spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres_ecommerce:5432/ecommerce_db") \
    .option("dbtable", "customers") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .option("driver", "org.postgresql.Driver") \
    .load()

def get_df_payments(spark):
    return spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres_ecommerce:5432/ecommerce_db") \
    .option("dbtable", "payments") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .option("driver", "org.postgresql.Driver") \
    .load()

def get_df_orders(spark):
    return spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres_ecommerce:5432/ecommerce_db") \
    .option("dbtable", "orders") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .option("driver", "org.postgresql.Driver") \
    .load()

def get_df_order_items(spark):
    return spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres_ecommerce:5432/ecommerce_db") \
    .option("dbtable", "order_items") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .option("driver", "org.postgresql.Driver") \
    .load()

def get_df_geolocation(spark):
    return spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres_ecommerce:5432/ecommerce_db") \
    .option("dbtable", "geolocation") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .option("driver", "org.postgresql.Driver") \
    .load()

