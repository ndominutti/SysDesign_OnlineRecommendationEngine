from datetime import datetime, timedelta
from airflow import DAG
import numpy as np
import pandas as pd
import os
from collections import defaultdict

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup


from src import dagsUtils
from src.topproduct import Topproduct
from src.topctr import Topctr


with DAG(
    dag_id="ad_recommender",
    schedule_interval="00 3 * * *",
    start_date=datetime(2023, 5, 10),
    catchup=False,
) as dag:
    with TaskGroup(group_id="filter_data") as FilterJob:
        FilterAdvertiserData = PythonOperator(
            task_id="AdvertiserData",
            python_callable=dagsUtils.filter_data,
            op_kwargs={
                "bucket_name": "ads-recommender-system",
                "raw_data_file_path": "input_data/ads_views.csv",
                "act_adv_file_path": "input_data/advertiser_ids.csv",
                "output_file_path": "airflow_subprocess_data/curated_ads_views.csv",
            },
        )

        FilterProductData = PythonOperator(
            task_id="ProductData",
            python_callable=dagsUtils.filter_data,
            op_kwargs={
                "bucket_name": "ads-recommender-system",
                "raw_data_file_path": "input_data/product_views.csv",
                "act_adv_file_path": "input_data/advertiser_ids.csv",
                "output_file_path": "airflow_subprocess_data/curated_product_views.csv",
            },
        )

    with TaskGroup(group_id="TrainJob") as TrainingJob:
        topproduct = PythonOperator(
            task_id="Topproduct",
            python_callable=dagsUtils.train_job,
            op_kwargs={
                "model": Topproduct,
                "bucket_name": "ads-recommender-system",
                "curated_data_file_path": "airflow_subprocess_data/curated_product_views.csv",
                "output_file_path": "airflow_subprocess_data/top_20_products.csv",
                "execution_date": "{{ execution_date }}",
            },
        )

        topctr = PythonOperator(
            task_id="TopCTR",
            python_callable=dagsUtils.train_job,
            op_kwargs={
                "model": Topctr,
                "bucket_name": "ads-recommender-system",
                "curated_data_file_path": "airflow_subprocess_data/curated_ads_views.csv",
                "output_file_path": "airflow_subprocess_data/top_20_ctr.csv",
                "execution_date": "{{ execution_date }}",
            },
        )

    with TaskGroup(group_id="WriteJob") as WriteJob:
        topproductDBWrite = PythonOperator(
            task_id="TopproductDBWrite",
            python_callable=dagsUtils.write_rds,
            op_kwargs={
                "bucket_name": "ads-recommender-system",
                "recommendation_file_path": "airflow_subprocess_data/top_20_products.csv",
                "model_type": "products",
            },
        )

        topCTRDBWrite = PythonOperator(
            task_id="TopCTRDBWrite",
            python_callable=dagsUtils.write_rds,
            op_kwargs={
                "bucket_name": "ads-recommender-system",
                "recommendation_file_path": "airflow_subprocess_data/top_20_ctr.csv",
                "model_type": "ctr",
            },
        )

    with TaskGroup(group_id="WriteHistoricJob") as WriteHistoricJob:
        topproductDBWriteHistoric = PythonOperator(
            task_id="TopproductDBWriteHistoric",
            python_callable=dagsUtils.write_historic,
            op_kwargs={
                "bucket_name": "ads-recommender-system",
                "recommendation_file_path": "airflow_subprocess_data/top_20_products.csv",
                "model_type": "products",
                "execution_date": "{{ execution_date }}",
            },
            provide_context=True,
        )

        topCTRDBWriteHistoric = PythonOperator(
            task_id="TopCTRDBWriteHistoric",
            python_callable=dagsUtils.write_historic,
            op_kwargs={
                "bucket_name": "ads-recommender-system",
                "recommendation_file_path": "airflow_subprocess_data/top_20_ctr.csv",
                "model_type": "ctr",
                "execution_date": "{{ execution_date }}",
            },
        )

    SendNotification = PythonOperator(
        task_id="SendNotification",
        python_callable=dagsUtils.send_sns_notification,
        op_kwargs={"execution_date": "{{ execution_date }}"},
    )

    FilterJob >> TrainingJob >> WriteJob >> WriteHistoricJob >> SendNotification
