from fastapi import FastAPI
from . import APIUtils

app = FastAPI()


@app.get("/")
def read_root():
    return {"Working": "API"}


@app.get("/recommendations/{ADV}/{Modelo}")
def recommendations(ADV: str, Modelo: str):
    return APIUtils.query_latest_recommendation(ADV, Modelo)
