import pandas as pd
from . import S3utils


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
