from pyspark.sql.functions import col, when, count, lit, coalesce

def apply_star_schema(df_products,
                    df_sellers,
                    df_orders,
                    df_customers,
                    df_geolocation,
                    df_order_items):
    
        # add customers_id to order_items
        df_fact_order_items = df_order_items.join(
        df_orders.select("order_id", "customer_id"),
        on="order_id",
        how="inner"
        )
        # drop customers_id from df_orders
        df_dim_orders = df_orders.drop("customer_id")

        
        return (df_products,
                df_sellers,
                df_dim_orders,
                df_customers,
                df_geolocation,
                df_fact_order_items)

def drop_columns(df_products,
                    df_sellers,
                    df_orders,
                    df_customers,
                    df_geolocation,
                    df_order_items):
        
        df_products_mod = df_products.drop("product_name_lenght", "product_description_lenght")
        df_orders_mod = df_orders.drop("order_approved_at", "order_delivered_carrier_date")

        # drop city and state from customers - can be found in geolocation too
        df_customers_mod = df_customers.drop("customer_city", "customer_state")
        return (df_products_mod,
                df_sellers,
                df_orders_mod,
                df_customers_mod,
                df_geolocation,
                df_order_items)

def clean_nulls(df_products,
                    df_sellers,
                    df_orders,
                    df_customers,
                    df_geolocation,
                    df_order_items):
        for df_name, df in locals().items():
                df.select([count(when(col(c).isNull() | (col(c) == ""), c)).alias(c)
                           for c in df.columns]).show()

# adds a volume column and removes the 3 length, height, width columns
def calculate_volume(df_products):
       return (df_products.withColumn(
              "product_volume_cm",
              coalesce(col("product_length_cm"), lit(1)) *
              coalesce(col("product_height_cm"), lit(1)) *
              coalesce(col("product_width_cm"), lit(1)))
              .drop("product_length_cm", "product_height_cm", "product_width_cm")) 

def apply_transformations(df_products,
                    df_sellers,
                    df_orders,
                    df_customers,
                    df_geolocation,
                    df_order_items):
    df_products = calculate_volume(df_products)
    dfs = [df_products, df_sellers, df_orders, df_customers, df_geolocation, df_order_items]

    dfs = drop_columns(*dfs)
    
    dfs = apply_star_schema(*dfs)

    clean_nulls(*dfs)
    return dfs