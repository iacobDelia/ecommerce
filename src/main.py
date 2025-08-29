
from utils.init_session import get_session
from etl.olist_db.extract import *;
from etl.olist_db.transform import apply_transformations;
from etl.olist_db.load import *;
# load the spark session
spark = get_session()

# extract all the dataframes
df_products = get_df_products(spark)
df_sellers = get_df_sellers(spark)
df_orders = get_df_orders(spark)
df_customers = get_df_customers(spark)
df_geolocation = get_df_geolocation(spark)
df_order_items = get_df_order_items(spark)


(df_products,
df_sellers,
df_orders,
df_customers,
df_geolocation,
df_order_items) = apply_transformations(df_products,
                                        df_sellers,
                                        df_orders,
                                        df_customers,
                                        df_geolocation,
                                        df_order_items)

load_in_snowflake(df_products,
                df_sellers,
                df_orders,
                df_customers,
                df_geolocation,
                df_order_items) 


