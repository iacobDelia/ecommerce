from config.snowflake_options import get_sf_options;

def load_in_snowflake(df_products,
                    df_sellers,
                    df_orders,
                    df_customers,
                    df_geolocation,
                    df_order_items):

    sf_options = get_sf_options();
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

    df_products.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", "DIM_PRODUCTS") \
        .mode("overwrite") \
        .save()

    df_sellers.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", "DIM_SELLERS") \
        .mode("overwrite") \
        .save()


    df_orders.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", "DIM_ORDERS") \
        .mode("overwrite") \
        .save()


    df_customers.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", "DIM_CUSTOMERS") \
        .mode("overwrite") \
        .save()


    df_geolocation.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", "DIM_GEOLOCATION") \
        .mode("overwrite") \
        .save()


    df_order_items.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", "FACT_ORDER_ITEMS") \
        .mode("overwrite") \
        .save()