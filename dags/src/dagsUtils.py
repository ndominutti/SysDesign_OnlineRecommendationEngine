import pandas as pd
from . import S3utils
import psycopg2


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


def write_rds(bucket_name, recommendation_file_path, model_type):
    recommendation = S3utils.get_data(bucket_name, recommendation_file_path)

    engine = psycopg2.connect(
        database="postgres",
        host="database-pa-udesa-test.c6z3l3m7uu0r.us-east-2.rds.amazonaws.com",
        user="postgres",
        password="udesa856",
        port=5432,
    )

    cursor = engine.cursor()
    if model_type == "products":
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS LATEST_PRODUCT_RECOMMENDATION (ADVERTISER VARCHAR(50),
                                                                                    PRODUCT VARCHAR(50),
                                                                                    DATE TIMESTAMP,
                                                                                    PRIMARY KEY (ADVERTISER, PRODUCT));"""
        )
        cursor.execute("""TRUNCATE TABLE LATEST_PRODUCT_RECOMMENDATION;""")
        for index, row in recommendation.iterrows():
            cursor.execute(
                """INSERT INTO LATEST_PRODUCT_RECOMMENDATION (ADVERTISER, PRODUCT, DATE) 
                                                    VALUES (%s, %s, %s)""",
                (row["advertiser_id"], row["product_id"], row["date"]),
            )

    engine.commit()
    cursor.close()
    engine.close()
    print("Insert Success")
