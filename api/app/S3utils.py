import boto3
from io import StringIO
import pandas as pd


def get_data(bucket_name: str, file_path: str) -> pd.DataFrame:
    """Download data from S3

    Args:
        bucket_name (str): name of the raw data bucket
        file_path (str): path to the file inside the bucket_name to be downloaded

    Returns:
        pd.DataFrame: S3 object as a dataframe
    """
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket_name, Key=file_path)
    return pd.read_csv(obj["Body"])


def post_data(bucket_name: str, file_path: str, data: pd.DataFrame) -> None:
    """Upload data to S3

    Args:
        bucket_name (str): name of the raw data bucket
        file_path (str): path to the file inside the bucket_name where to write the data
        data (pd.DataFrame): dataframe to be written
    """
    s3 = boto3.resource("s3")
    buffer = StringIO()
    data.to_csv(buffer, index=False)
    s3.Object(bucket_name, file_path).put(Body=buffer.getvalue())
    print("Upload Success")
