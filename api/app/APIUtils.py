import pandas as pd

import psycopg2
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = f"{current_dir}/../../"
sys.path.append(parent_dir)
print(sys.path)
from dags.src import S3utils


def query_latest_recommendation(
    advertiser_id: str, model_type: str, engine: psycopg2.connect
) -> dict:
    """
    Query last recommendation in RDS table

    Args:
        advertiser_id(str): advertiser_id to get recommendations for
        model_type(str): products or ctr
        engine(psycopg2.connect): engine to conect to RDS Postgres

    Returns:
        dict: recommendations
    """
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
    return {"recommendations": dataframe["product"].values.tolist()}


def query_historic_recommendation(
    advertiser_id: str, model_type: str, engine: str
) -> dict:
    """
    Return recommendation from the past 7 days

    Args:
        advertiser_id(str): advertiser_id to get recommendations for
        model_type(str): products or ctr
        engine(psycopg2.connect): engine to conect to RDS Postgres

    Returns:
        dict: recommendations
    """
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
    return {"recommendations": returnable}


def _stat_cantidades(dataframe: pd.DataFrame, column: str) -> pd.DataFrame:
    """Calculate unique count from a given colum

    Args:
        dataframe (pd.DataFrame): S3 raw data
        column (str): data where to perform unique on

    Returns:
        pd.DataFrame: stats
    """
    return dataframe[column].nunique()


def _stat_last_week_variation_rate(dataframe: pd.DataFrame) -> dict:
    """Calulate last week variational rate for advertisers

    Args:
        dataframe (pd.DataFrame): RDS historial table dataframe

    Returns:
        dict: stats
    """
    variation_order = (
        dataframe.groupby("advertiser").product.nunique()
        / dataframe.groupby("advertiser").product.size()
    ).sort_values(ascending=False)
    return {
        "order": variation_order.index.values.tolist(),
        "score": variation_order.values.tolist(),
    }


def _stat_model_coincidence(
    dataframe_prods: pd.DataFrame, dataframe_advs: pd.DataFrame
) -> dict:
    """Calulate coincidence between models

    Args:
        dataframe_prods (pd.DataFrame): RDS latest products table dataframe
        dataframe_advs (pd.DataFrame): RDS latest ads table dataframe

    Returns:
        dict: stats
    """
    df = pd.concat([dataframe_prods, dataframe_advs])
    df = (
        1
        - df.groupby("advertiser").product.nunique()
        / df.groupby("advertiser").size()[0]
    ).sort_values(ascending=False)
    return {"order": df.index.values.tolist(), "score": df.values.tolist()}


def _stat_weekly_repeated_products(
    dataframe_prods: pd.DataFrame, dataframe_advs: pd.DataFrame
) -> dict:
    """Calulate weekly repeated products for each model

    Args:
        dataframe_prods (pd.DataFrame): RDS historical products table dataframe
        dataframe_advs (pd.DataFrame): RDS historical ads table dataframe

    Returns:
        dict: stats
    """
    dataframe_prods = (
        dataframe_prods.groupby(["advertiser"])
        .product.value_counts()
        .reset_index()
        .groupby("advertiser")
        .head()
    )
    dataframe_advs = (
        dataframe_advs[dataframe_advs.ctr > 0]
        .groupby(["advertiser"])
        .product.value_counts()
        .reset_index()
        .groupby("advertiser")
        .head()
    )

    return (
        {
            "advertiser": dataframe_prods.advertiser.values.tolist(),
            "order": dataframe_prods["product"].values.tolist(),
            "score": dataframe_prods["count"].values.tolist(),
        },
        {
            "advertiser": dataframe_advs.advertiser.values.tolist(),
            "order": dataframe_advs["product"].values.tolist(),
            "score": dataframe_advs["count"].values.tolist(),
        },
    )


def stats_factory(engine: psycopg2.connect) -> dict:
    """
    Calculate and show recommendation stats.

    Args:
        engine(psycopg2.connect):engine to conect to RDS Postgres

    Returns:
        dict: stats
    """
    dataframe_prod_s3 = S3utils.get_data(
        bucket_name="ads-recommender-system",
        file_path="airflow_subprocess_data/curated_product_views.csv",
    )
    dataframe_advs_s3 = S3utils.get_data(
        bucket_name="ads-recommender-system",
        file_path="airflow_subprocess_data/curated_ads_views.csv",
    )

    dataframe_prod_rds_hist = pd.read_sql(
        f"""SELECT * FROM HISTORIC_PRODUCT_RECOMMENDATION WHERE DATE >= CURRENT_DATE - INTERVAL '7 days';""",
        engine,
    )
    dataframe_advs_rds_hist = pd.read_sql(
        f"""SELECT * FROM HISTORIC_ADVERTISERS_RECOMMENDATION WHERE DATE >= CURRENT_DATE - INTERVAL '7 days';""",
        engine,
    )

    dataframe_prod_rds = pd.read_sql(
        f"""SELECT * FROM LATEST_PRODUCT_RECOMMENDATION;""",
        engine,
    )
    dataframe_advs_rds = pd.read_sql(
        f"""SELECT * FROM LATEST_ADVERTISERS_RECOMMENDATION;""",
        engine,
    )

    weekly_top5_products_by_advertiser = _stat_weekly_repeated_products(
        dataframe_prod_rds_hist, dataframe_advs_rds_hist
    )

    return {
        "advertisers_count_data_raw": {
            "products": _stat_cantidades(dataframe_prod_s3, "advertiser_id"),
            "ctr": _stat_cantidades(dataframe_advs_s3, "advertiser_id"),
        },
        "products_count_data_raw": {
            "products": _stat_cantidades(dataframe_prod_s3, "product_id"),
            "ctr": _stat_cantidades(dataframe_advs_s3, "product_id"),
        },
        "last_week_variation": {
            "products": _stat_last_week_variation_rate(dataframe_prod_rds_hist),
            "ctr": _stat_last_week_variation_rate(dataframe_advs_rds_hist),
        },
        "model_coincidence_by_advertiser": _stat_model_coincidence(
            dataframe_prod_rds, dataframe_advs_rds
        ),
        "weekly_top5_products_by_advertiser": {
            "products": weekly_top5_products_by_advertiser[0],
            "ctr": weekly_top5_products_by_advertiser[1],
        },
    }
