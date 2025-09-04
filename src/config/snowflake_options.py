import os
from dotenv import load_dotenv

load_dotenv()

def get_sf_options():
    return {
        "sfURL": os.getenv("SF_URL"),
        "sfUser": os.getenv("SF_USER"),
        "sfPassword": os.getenv("SF_PASSWORD"),
        "sfDatabase": os.getenv("SF_DATABASE"),
        "sfSchema": os.getenv("SF_SCHEMA"),
        "sfWarehouse": os.getenv("SF_WAREHOUSE"),
        "sfRole": os.getenv("SF_ROLE"),
    }
