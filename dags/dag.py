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
from src import S3utils
from src.topproduct import Topproduct
from src.topctr import Topctr

# # Obtener la fecha actual
# today = datetime.now()

# # Restar 4 días
# days_ago = today - timedelta(days=4)

# # Formatear la fecha en el formato "año-mes-día"
# date_str = days_ago.strftime('%Y-%m-%d')


with DAG(
    dag_id="ad_recommender",
    schedule_interval=None,  # Solo corre cuando lo ejecuto a mano desde el webserver
    # schedule_interval=timedelta(minutes=1),
    start_date=datetime(2023, 4, 29),
    catchup=False,
) as dag:
    with TaskGroup(group_id="download") as group1:
        download_data = PythonOperator(
            task_id="DownloadData",
            python_callable=S3utils.get_data,
            op_kwargs={
                "bucket_name": "ad-recommender-system",
                "file_path": "/input_data/ads_views.csv",
            },
        )

        download_data2 = PythonOperator(
            task_id="DownloadData2",
            python_callable=S3utils.get_data,
            op_kwargs={
                "bucket_name": "ad-recommender-system",
                "file_path": "/input_data/product_views.csv",
            },
        )

        download_data3 = PythonOperator(
            task_id="DownloadData3",
            python_callable=S3utils.get_data,
            op_kwargs={
                "bucket_name": "ad-recommender-system",
                "file_path": "/input_data/advertiser_id.csv",
            },
        )

    with TaskGroup(group_id="filter") as group2:
        filter_data1 = PythonOperator(
            task_id="FiltrarDatos",
            python_callable=dagsUtils.filtrar_advertiser,
            op_kwargs={"raw_data": download_data, "active_advertisers": download_data3},
        )

        filter_data2 = PythonOperator(
            task_id="FiltrarDatos2",
            python_callable=dagsUtils.filtrar_advertiser,
            op_kwargs={
                "raw_data": download_data2,
                "active_advertisers": download_data3,
            },
        )
    with TaskGroup(group_id="upload") as group3:
        upload1 = PythonOperator(
            task_id="SubirDatos",
            python_callable=S3utils.post_data,
            op_kwargs={
                "bucket_name": "ad-recommender-system",
                "file_path": "/airflow_subprocess_data/advertiser.csv",
                "data": filter_data1,
            },
        )
        upload2 = PythonOperator(
            task_id="SubirDatos2",
            python_callable=S3utils.post_data,
            op_kwargs={
                "bucket_name": "ad-recommender-system",
                "file_path": "/airflow_subprocess_data/products.csv",
                "data": filter_data2,
            },
        )

    with TaskGroup(group_id="download2") as group4:
        download4 = PythonOperator(
            task_id="DownloadData4",
            python_callable=S3utils.get_data,
            op_kwargs={
                "bucket_name": "ad-recommender-system",
                "file_path": "/airflow_subprocess_data/advertiser.csv",
            },
        )
        download5 = PythonOperator(
            task_id="DownloadData5",
            python_callable=S3utils.get_data,
            op_kwargs={
                "bucket_name": "ad-recommender-system",
                "file_path": "/airflow_subprocess_data/products.csv",
            },
        )

    with TaskGroup(group_id="models") as group5:
        topproduct = PythonOperator(
            task_id="Topproduct",
            python_callable=Topproduct,
            op_kwargs={"data": download5},
        )
        topctr = PythonOperator(
            task_id="TopCTR", python_callable=Topctr, op_kwargs={"data": download4}
        )

    # with TaskGroup(group_id = "DBWriting") as group6:
    #     write1 = PythonOperator(
    #                                 task_id='DBWriting1',
    #                                 python_callable=dagsUtils.post_data,
    #                                 op_kwargs={'bucket_name':"ad-recommender-system",
    #                                         'file_path':"/airflow_subprocess_data/advertiser.csv",
    #                                         'data':filter_data1})
    #     write2 = PythonOperator(
    #                                 task_id='DBWriting2',
    #                                 python_callable=dagsUtils.post_data,
    #                                 op_kwargs={'bucket_name':"ad-recommender-system",
    #                                         'file_path':"/airflow_subprocess_data/products.csv",
    #                                         'data':filter_data2})

    group1 >> group2
    group2 >> group3
    group3 >> group4
    group4 >> group5
    # group5 >> group6
