from fastapi import FastAPI
import psycopg2
from . import APIUtils
import os

app = FastAPI()

engine = psycopg2.connect(
    database="postgres",
    host=os.getenv("POSTGRES_HOST"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASS"),
    port=5432,
)


@app.get("/")
def read_root():
    return {"Working": "API"}


@app.get("/recommendations/{ADV}/{Modelo}")
def recommendations(ADV: str, Modelo: str) -> dict:
    """Return recommendations for the given advertiser by
    a given model

    Args:
        ADV (str): advertiser ID
        Modelo (str): model type, can be products or ctr
    """
    if not Modelo in ["products", "ctr"]:
        return {"Error": 'El Modelo debe ser "products" o "ctr"'}
    else:
        return APIUtils.query_latest_recommendation(ADV, Modelo, engine)


@app.get("/stats/")
def stats() -> dict:
    """Calculate and return stats

    Returns:
        dict: stats
    """
    return APIUtils.stats_factory(engine)


@app.get("/history/{ADV}/{Modelo}")
def history(ADV: str, Modelo: str) -> dict:
    """Return last 7 days recommendations for the given advertiser by
    a given model

    Args:
        ADV (str): advertiser ID
        Modelo (str): model type, can be products or ctr

    Returns:
        dict: last 7 days recommendations
    """
    if not Modelo in ["products", "ctr"]:
        return {"Error": 'El Modelo debe ser "products" o "ctr"'}
    else:
        return APIUtils.query_historic_recommendation(ADV, Modelo, engine)
