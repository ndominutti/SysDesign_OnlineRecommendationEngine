import pandas as pd


def query_latest_recommendation(advertiser_id, model_type, engine):
    table_names = {
        "products": "LATEST_PRODUCT_RECOMMENDATION",
        "ctr": "LATEST_ADVERTISERS_RECOMMENDATION",
    }
    query = f"""SELECT * FROM {table_names[model_type]} WHERE ADVERTISER = '{advertiser_id}'"""
    dataframe = pd.read_sql(query, engine)
    return_data = {advertiser_id: dataframe.product.values.tolist()}
