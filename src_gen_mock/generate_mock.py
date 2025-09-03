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
        "product_name_lenght": random.randrange(15, 70),
        "product_description_lenght": random.randrange(100, 4000),
        "product_photos_qty": random.randrange(0, 4),
        "product_weight_g": random.randrange(20, 200) * 10,
        "product_length_cm": random.randrange(2, 50),
        "product_height_cm": random.randrange(2, 50),
        "product_width_cm": random.randrange(2, 50)
    }
    return generated_product

def generate_customer():
    # extract the zip codes
    with open("./src_gen_mock/sample/zip_codes_prefix.json", 'r') as f:
        data = json.load(f)
    
    zip_codes_list = [item["geolocation_zip_code_prefix"] for item in data["zip_codes"]]
    # generate random values
    generated_customer = {
        "customer_zip_code_prefix": random.choice(zip_codes_list)
        

    }