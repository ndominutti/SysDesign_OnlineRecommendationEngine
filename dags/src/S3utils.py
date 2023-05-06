import boto3
from io import StringIO
import pandas as pd


def get_data(**context):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=context["bucket_name"], Key=context["file_path"])
    key = context["file_path"].split("/")[-1].replace(".csv", "")
    context[key].xcom_push(key="data", value=pd.read_csv(obj["Body"]))
    # return pd.read_csv(obj["Body"]).to_dict()


def post_data(bucket_name, file_path, data):
    s3 = boto3.resource("s3")
    buffer = StringIO()
    data.to_csv(buffer, index=False)
    s3.Object(bucket_name, file_path).put(Body=buffer.getvalue())
    print("Upload Success")
