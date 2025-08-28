
from config.snowflake_options import get_sf_options;
from utils.init_session import get_session
from etl.olist_db.extract import *;

# load the spark session
spark = get_session()

# extract all the dataframes
df_products = get_df_products(spark)
df_sellers = get_df_sellers(spark)
df_orders = get_df_orders(spark)
df_customers = get_df_customers(spark)
df_geolocation = get_df_geolocation(spark)
df_order_items = get_df_order_items(spark)


sf_options = get_sf_options();

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

df_geolocation.limit(20).show()
df_orders.show()
df_customers.show()
