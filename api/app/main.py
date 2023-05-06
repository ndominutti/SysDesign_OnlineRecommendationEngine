from fastapi import FastAPI
import APIUtils

app = FastAPI()


@app.get("/")
def read_root():
    return {"Working": "API"}


@app.get(f"/recommendations/{ADV}/{Modelo}")
def recommendations(ADV: str, Modelo: str):
    return APIUtils.query_latest_recommendation(ADV, Modelo)
