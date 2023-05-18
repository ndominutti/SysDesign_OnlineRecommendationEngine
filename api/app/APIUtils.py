import pandas as pd
import psycopg2
from . import S3utils


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
    query = f"""SELECT * FROM {table_names[model_type]} WHERE ADVERTISER = '{advertiser_id}' AND DATE >= CURRENT_DATE - INTERVAL '7 days';"""
    dataframe = pd.read_sql(query, engine)
    if dataframe.shape[0] < 0:
        return {
            "Error": "El advertiser ingresado no es válido o no existe en la base de datos"
        }
    dataframe = dataframe.groupby("date").product.unique().to_dict()
    returnable = {k: list(v) for k, v in dataframe.items()}
    return {advertiser_id: returnable}


def _stat_cantidades(dataframe, column):
    return dataframe[column].nunique()


def _stat_last_week_variation_rate(dataframe):
    variation_order = (
        dataframe.groupby("advertiser").product.nunique()
        / dataframe.groupby("advertiser").product.size()
    ).sort_values(ascending=False)
    return {
        "variation_order": variation_order.index.values.tolist(),
        "variation_score": variation_order.values.tolist(),
    }


def stats_factory(engine):
    dataframe_prod_s3 = S3utils.get_data(
        bucket_name="ads-recommender-system",
        file_path="airflow_subprocess_data/curated_product_views.csv",
    )
    dataframe_advs_s3 = S3utils.get_data(
        bucket_name="ads-recommender-system",
        file_path="airflow_subprocess_data/curated_ads_views.csv",
    )

    dataframe_prod_rds = pd.read_sql(
        f"""SELECT * FROM HISTORIC_PRODUCT_RECOMMENDATION WHERE DATE >= CURRENT_DATE - INTERVAL '7 days';""",
        engine,
    )
    dataframe_advs_rds = pd.read_sql(
        f"""SELECT * FROM HISTORIC_ADVERTISERS_RECOMMENDATION WHERE DATE >= CURRENT_DATE - INTERVAL '7 days';""",
        engine,
    )

    return {
        "Cantidad_Advertisers_data_raw": {
            "products": _stat_cantidades(dataframe_prod_s3, "advertiser_id"),
            "ctr": _stat_cantidades(dataframe_advs_s3, "advertiser_id"),
        },
        "Cantidad_de_productos_data_raw": {
            "products": _stat_cantidades(dataframe_prod_s3, "product_id"),
            "ctr": _stat_cantidades(dataframe_advs_s3, "product_id"),
        },
        "last_week_variation": {
            "products": _stat_last_week_variation_rate(dataframe_prod_rds),
            "ctr": _stat_last_week_variation_rate(dataframe_advs_rds),
        },
    }
