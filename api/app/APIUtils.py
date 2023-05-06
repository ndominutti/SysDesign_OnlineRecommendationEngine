import psycopg2
import pandas as pd


def query_latest_recommendation(advertiser_id, model_type):
    engine = psycopg2.connect(
        database="postgres",
        host="database-pa-udesa-test.c6z3l3m7uu0r.us-east-2.rds.amazonaws.com",
        user="postgres",
        password="udesa856",
        port=5432,
    )
    table_names = {
        "products": "LATEST_PRODUCT_RECOMMENDATION",
        "ctr": "LATEST_ADVERTISERS_RECOMMENDATION",
    }
    query = f"""SELECT * FROM {table_names[model_type]}"""
    return pd.read_sql(query, engine).to_dict()
