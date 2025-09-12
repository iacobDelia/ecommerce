from pyspark.sql import functions as F

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
        how="inner")
        
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
        
        # drop irrelevant columns
        df_products_mod = df_products.drop("product_name_length", "product_description_length")
        df_orders_mod = df_orders.drop("order_approved_at", "order_delivered_carrier_date")

        # drop city and state from customers - can be found in geolocation too
        df_customers_mod = df_customers.drop("customer_city", "customer_state")

        # delete old metrics
        df_order_items = df_order_items.filter(F.col("shipping_limit_date") > F.to_date(F.lit("2016-01-01")))
        return (df_products_mod,
                df_sellers,
                df_orders_mod,
                df_customers_mod,
                df_geolocation,
                df_order_items)

# shows the number of nulls or blank strings for each column
def show_nulls(df_products,
                    df_sellers,
                    df_orders,
                    df_customers,
                    df_geolocation,
                    df_order_items):
        for df_name, df in locals().items():
                df.select([F.count(F.when(F.col(c).isNull() | (F.col(c) == ""), c)).alias(c)
                           for c in df.columns]).show()
        
        df_order_items.orderBy(F.col("shipping_limit_date").desc()).show()

# cleans the nulls from some of the tables
def clean_nulls(df_products,
                    df_sellers,
                    df_orders,
                    df_customers,
                    df_geolocation,
                    df_order_items):
        # replace nulls and empty strings in product category name
        df_products = df_products.withColumn(
               "product_category_name",
               F.when(
                      ((F.col("product_category_name").isNull()) | (F.col("product_category_name") == "")),
                      "UNKNOWN")
                      .otherwise(F.col("product_category_name"))
               
        )
        df_products = df_products.fillna({"product_photos_qty": 0})

        return (df_products,
                df_sellers,
                df_orders,
                df_customers,
                df_geolocation,
                df_order_items)
        
# adds a volume column and removes the 3 length, height, width columns
def calculate_volume(df_products):
       return (df_products.withColumn(
              "product_volume_cm",
              F.coalesce(F.col("product_length_cm"), F.lit(1)) *
              F.coalesce(F.col("product_height_cm"), F.lit(1)) *
              F.coalesce(F.col("product_width_cm"), F.lit(1)))
              .drop("product_length_cm", "product_height_cm", "product_width_cm")) 

# the dates in the original dataset were all around 2019
# but when adding new entries in the database, theyre put as the current date
# they need to be corrected for more accurate statistics
def correct_shipping_date(df_order_items):
       return df_order_items.withColumn(
              "shipping_limit_date",
              F.when(
                     F.col("shipping_limit_date") > F.to_date(F.lit("2019-12-31")),
                     F.add_months(F.col("shipping_limit_date"), -84)
              ).otherwise(F.col("shipping_limit_date"))
       )

def correct_purchase_timestamp(df_orders):
       return df_orders.withColumn(
              "order_purchase_timestamp",
              F.when(
                     F.col("order_purchase_timestamp") > F.to_date(F.lit("2019-12-31")),
                     F.add_months(F.col("order_purchase_timestamp"), -84)
              ).otherwise(F.col("order_purchase_timestamp"))
       ).withColumn(
              "order_delivered_customer_date",
              F.when(
                     F.col("order_delivered_customer_date") > F.to_date(F.lit("2019-12-31")),
                     F.add_months(F.col("order_delivered_customer_date"), -84)
              ).otherwise(F.col("order_delivered_customer_date"))
       )


# applies all the transformations
def apply_transformations(df_products,
                    df_sellers,
                    df_orders,
                    df_customers,
                    df_geolocation,
                    df_order_items):
    df_products = calculate_volume(df_products)
    df_order_items = correct_shipping_date(df_order_items)
    df_orders = correct_purchase_timestamp(df_orders)
    dfs = [df_products, df_sellers, df_orders, df_customers, df_geolocation, df_order_items]
    
    dfs = drop_columns(*dfs)
    dfs = clean_nulls(*dfs)
    dfs = apply_star_schema(*dfs)

    show_nulls(*dfs)
    return dfs