import pandas as pd
from . import S3utils
import psycopg2
from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.base_hook import BaseHook
import boto3


def filter_data(
    bucket_name: str,
    raw_data_file_path: str,
    act_adv_file_path: str,
    output_file_path: str,
) -> None:
    """Filter data given an active advertisers file

    Args:
        bucket_name (str): name of the raw data bucket
        raw_data_file_path (str): path to the raw data inside the bucket_name to be filtered
        act_adv_file_path (str): path to the active advertisers data inside the bucket_name to filter raw data
        output_file_path (str): output path inside the bucket_name
    """
    raw_data = S3utils.get_data(bucket_name, raw_data_file_path)
    act_adv = S3utils.get_data(bucket_name, act_adv_file_path)
    filtered_data = raw_data[raw_data["advertiser_id"].isin(act_adv.advertiser_id)]
    S3utils.post_data(bucket_name, output_file_path, filtered_data)


def train_job(
    model: str,
    bucket_name: str,
    curated_data_file_path: str,
    output_file_path: str,
    execution_date: str,
) -> None:
    """Create ranking order with the models

    Args:
        model (str): model type, can be product or ctr
        bucket_name (str): name of the raw data bucket
        curated_data_file_path (str): path to the curated data inside the bucket_name to be filtered
        output_file_path (str): output path inside the bucket_name
        execution_date (str): excecution_date from airflow dag
    """
    curated_data = S3utils.get_data(bucket_name, curated_data_file_path)
    model_instance = model(curated_data)
    excecution_previous_date = str(
        datetime.strptime(execution_date.split("T")[0], "%Y-%m-%d") - timedelta(days=1)
    ).split(" ")[0]
    print(excecution_previous_date)
    recommendation = model_instance.top_20(excecution_previous_date)
    S3utils.post_data(bucket_name, output_file_path, recommendation)


def write_historic(
    bucket_name: str,
    recommendation_file_path: str,
    model_type: str,
    execution_date: str,
) -> None:
    """Write a file into the historic DDBB

    Args:
        bucket_name (str): name of the raw data bucket
        recommendation_file_path (str): path to the recommendation data inside the bucket_name to be written
        model_type (str): type of the model to be written, can be products or ctr
        execution_date (str): excecution_date from airflow dag
    """
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
    if model_type == "products":
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS HISTORIC_PRODUCT_RECOMMENDATION (ADVERTISER VARCHAR(50),
                                                                            PRODUCT VARCHAR(50),
                                                                            DATE date,
                                                                            EVENT_COUNT integer,
                                                                            PRIMARY KEY (ADVERTISER, PRODUCT, DATE));"""
        )
        cursor.execute(
            f"""DELETE FROM HISTORIC_PRODUCT_RECOMMENDATION WHERE DATE='{execution_date.split(" ")[0].split("T")[0]}';""",
        )
        for index, row in recommendation.iterrows():
            cursor.execute(
                """INSERT INTO HISTORIC_PRODUCT_RECOMMENDATION (ADVERTISER, PRODUCT, DATE, EVENT_COUNT) 
                                                    VALUES (%s, %s, %s, %s);""",
                (
                    row["advertiser_id"],
                    row["product_id"],
                    execution_date.split(" ")[0].split("T")[0],
                    row["event_count"],
                ),
            )
    else:
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS HISTORIC_ADVERTISERS_RECOMMENDATION (ADVERTISER VARCHAR(50),
                                                                            PRODUCT VARCHAR(50),
                                                                            DATE date,
                                                                            CTR float8,
                                                                            PRIMARY KEY (ADVERTISER, PRODUCT, DATE));"""
        )
        cursor.execute(
            f"""DELETE FROM HISTORIC_ADVERTISERS_RECOMMENDATION WHERE DATE='{execution_date.split(" ")[0].split("T")[0]}';""",
        )
        for index, row in recommendation.iterrows():
            cursor.execute(
                """INSERT INTO HISTORIC_ADVERTISERS_RECOMMENDATION (ADVERTISER, PRODUCT, DATE, CTR) 
                                                    VALUES (%s, %s, %s, %s);""",
                (
                    row["advertiser_id"],
                    row["product_id"],
                    execution_date.split(" ")[0].split("T")[0],
                    row["CTR"],
                ),
            )

    engine.commit()
    cursor.close()
    engine.close()
    print("Insert Success")


def write_rds(bucket_name: str, recommendation_file_path: str, model_type: str) -> None:
    """Write a file into the latest DDBB

    Args:
        bucket_name (str): name of the raw data bucket
        recommendation_file_path (str): path to the recommendation data inside the bucket_name to be written
        model_type (str): type of the model to be written, can be products or ctr
    """
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


def send_sns_notification(execution_date: str) -> None:
    """Send an email notificating the workflow has succeded

    Args:
        execution_date (str): excecution_date from airflow dag
    """
    # Specify the SNS topic ARN
    sns_topic_arn = "arn:aws:sns:us-east-2:169385451286:airflow_recsys"

    # Create the SNS message
    message = f"Group Dominutti-SuarezGurruchaga-Telechea's airflow job succeeded at {execution_date}."

    # Publish the SNS message
    sns_client = boto3.client("sns", region_name="us-east-2")
    sns_client.publish(TopicArn=sns_topic_arn, Message=message)
