import pandas as pd


def query_latest_recommendation(advertiser_id, model_type, engine):
    if not model_type in ["products", "ctr"]:
        return {"Error": 'El model_type debe ser "products" o "ctr"'}
    table_names = {
        "products": "LATEST_PRODUCT_RECOMMENDATION",
        "ctr": "LATEST_ADVERTISERS_RECOMMENDATION",
    }
    query = f"""SELECT * FROM {table_names[model_type]} WHERE ADVERTISER = '{advertiser_id}'"""
    dataframe = pd.read_sql(query, engine)
    if dataframe.shape[0] < 0:
        return {
            "Error": "El advertiser ingresado no es válido o no existe en la base de datos"
        }
    return {advertiser_id: dataframe["product"].values.tolist()}


def query_historic_recommendation(advertiser_id, model_type, engine):
    """
    Return recommendation from the past 7 days
    """
    if not model_type in ["products", "ctr"]:
        return {"Error": 'El model_type debe ser "products" o "ctr"'}
    table_names = {
        "products": "HISTORIC_PRODUCT_RECOMMENDATION",
        "ctr": "HISTORIC_ADVERTISERS_RECOMMENDATION",
    }
    query = f"""SELECT * FROM {table_names[model_type]} WHERE ADVERTISER = '{advertiser_id}' AND DATE <= CURRENT_DATE - INTERVAL '7 days';"""
    dataframe = pd.read_sql(query, engine)
    if dataframe.shape[0] < 0:
        return {
            "Error": "El advertiser ingresado no es válido o no existe en la base de datos"
        }
    dataframe = dataframe.groupby("date").product.unique().to_dict()
    returnable = {k: list(v) for k, v in dataframe.items()}
    return {advertiser_id: returnable}
