import pandas as pd
from . import S3utils
import psycopg2
from datetime import datetime


def filter_data(bucket_name, raw_data_file_path, act_adv_file_path, output_file_path):
    raw_data = S3utils.get_data(bucket_name, raw_data_file_path)
    act_adv = S3utils.get_data(bucket_name, act_adv_file_path)
    filtered_data = raw_data[raw_data["advertiser_id"].isin(act_adv.advertiser_id)]
    S3utils.post_data(bucket_name, output_file_path, filtered_data)


def train_job(model, bucket_name, curated_data_file_path, output_file_path):
    curated_data = S3utils.get_data(bucket_name, curated_data_file_path)
    model_instance = model(curated_data)
    recommendation = model_instance.top_20()
    S3utils.post_data(bucket_name, output_file_path, recommendation)


def write_historic(bucket_name, recommendation_file_path, model_type):
    assert model_type in [
        "products",
        "ctr",
    ], 'model_type can only recieve "products" or "ctr"'
    recommendation = S3utils.get_data(bucket_name, recommendation_file_path)

    engine = psycopg2.connect(
        database="postgres",
        host="db-airflow.c6z3l3m7uu0r.us-east-2.rds.amazonaws.com",
        user="postgres_admin",
        password="udesa856",
        port=5432,
    )

    cursor = engine.cursor()
    engine.begin()
    if model_type == "products":
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS HISTORIC_PRODUCT_RECOMMENDATION (ADVERTISER VARCHAR(50),
                                                                            PRODUCT VARCHAR(50),
                                                                            DATE TIMESTAMP,
                                                                            EVENT_COUNT integer,
                                                                            PRIMARY KEY (ADVERTISER, PRODUCT, DATE));"""
        )
        for index, row in recommendation.iterrows():
            cursor.execute(
                """INSERT INTO HISTORIC_PRODUCT_RECOMMENDATION (ADVERTISER, PRODUCT, DATE, EVENT_COUNT) 
                                                    VALUES (%s, %s, %s, %s)""",
                (
                    row["advertiser_id"],
                    row["product_id"],
                    datetime.now(),
                    row["event_count"],
                ),
            )
    else:
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS HISTORIC_ADVERTISERS_RECOMMENDATION (ADVERTISER VARCHAR(50),
                                                                            PRODUCT VARCHAR(50),
                                                                            DATE TIMESTAMP,
                                                                            CTR float8,
                                                                            PRIMARY KEY (ADVERTISER, PRODUCT, DATE));"""
        )
        for index, row in recommendation.iterrows():
            cursor.execute(
                """INSERT INTO HISTORIC_ADVERTISERS_RECOMMENDATION (ADVERTISER, PRODUCT, DATE, CTR) 
                                                    VALUES (%s, %s, %s, %s)""",
                (
                    row["advertiser_id"],
                    row["product_id"],
                    datetime.now(),
                    row["CTR"],
                ),
            )

    engine.commit()
    cursor.close()
    engine.close()
    print("Insert Success")


def write_rds(bucket_name, recommendation_file_path, model_type):
    assert model_type in [
        "products",
        "ctr",
    ], 'model_type can only recieve "products" or "ctr"'
    recommendation = S3utils.get_data(bucket_name, recommendation_file_path)
    print("OK")
    engine = psycopg2.connect(
        database="postgres",
        host="db-airflow.c6z3l3m7uu0r.us-east-2.rds.amazonaws.com",
        user="postgres_admin",
        password="udesa856",
        port=5432,
    )

    cursor = engine.cursor()
    engine.begin()
    if model_type == "products":
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS LATEST_PRODUCT_RECOMMENDATION (ADVERTISER VARCHAR(50),
                                                                                    PRODUCT VARCHAR(50),
                                                                                    EVENT_COUNT integer,
                                                                                    PRIMARY KEY (ADVERTISER, PRODUCT));"""
        )
        print("OK1")
        cursor.execute("""TRUNCATE TABLE LATEST_PRODUCT_RECOMMENDATION;""")
        print("OK2")
        for index, row in recommendation.iterrows():
            cursor.execute(
                """INSERT INTO LATEST_PRODUCT_RECOMMENDATION (ADVERTISER, PRODUCT, EVENT_COUNT) 
                                                    VALUES (%s, %s, %s)""",
                (row["advertiser_id"], row["product_id"], row["event_count"]),
            )
        print("OK3")
    else:
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS LATEST_ADVERTISERS_RECOMMENDATION (ADVERTISER VARCHAR(50),
                                                                            PRODUCT VARCHAR(50),
                                                                            CTR float8,
                                                                            PRIMARY KEY (ADVERTISER, PRODUCT));"""
        )
        cursor.execute("""TRUNCATE TABLE LATEST_ADVERTISERS_RECOMMENDATION;""")
        for index, row in recommendation.iterrows():
            cursor.execute(
                """INSERT INTO LATEST_ADVERTISERS_RECOMMENDATION (ADVERTISER, PRODUCT, CTR) 
                                                    VALUES (%s, %s, %s)""",
                (row["advertiser_id"], row["product_id"], row["CTR"]),
            )

    engine.commit()
    cursor.close()
    engine.close()
    print("Insert Success")
