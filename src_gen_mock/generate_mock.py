import json
import random

def generate_product():
    # extract the category names for a product in a list
    with open("./src_gen_mock/sample/product_category_names.json", 'r') as f:
        data = json.load(f)

    category_list = [item["product_category_name"] for item in data["product_category_name_list"]]

    # generate random values
    generated_product = {
        "product_category_name": random.choice(category_list),
        "product_name_length": random.randrange(15, 70),
        "product_description_length": random.randrange(100, 4000),
        "product_photos_qty": random.randrange(0, 4),
        "product_weight_g": random.randrange(20, 200) * 10,
        "product_length_cm": random.randrange(2, 50),
        "product_height_cm": random.randrange(2, 50),
        "product_width_cm": random.randrange(2, 50)
    }
    return generated_product

def generate_customer():
    # extract the zip codes
    with open("./src_gen_mock/sample/geolocation_data.json", 'r') as f:
        data = json.load(f)
    
    location_list = data["locations"]
    location = random.choice(location_list)
    # generate random values
    generated_customer = {
        "customer_zip_code_prefix": location["geolocation_zip_code_prefix"],
        "customer_city": location["geolocation_city"],
        "customer_state": location["geolocation_state"]
    }
    return generated_customer

def generate_order_item(order_id, order_item_id, product_id, seller_id):
    order_item = {
        "order_id": order_id,
        "order_item_id": order_item_id,
        "product_id": product_id,
        "seller_id": seller_id,
        "price": random.randrange(7, 400),
        "freight_value": random.randrange(7, 25)
    }
    return order_item

def generate_order(customer_id):
    generated_order = {
        "customer_id": customer_id,
        "order_status": "delivered",
    }
    return generated_order