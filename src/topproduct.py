import pandas as pd
import S3utils as s3

class Topproduct():
    def __init__(self, data):
        self.data = data
    def top_20(self):
        self.data = self.data.groupby(["advertiser_id", "product_id"]).count().reset_index()
        self.data = self.data.sort_values(["advertiser_id", "date"], ascending= False)
        return self.data.groupby("advertiser_id").head(20)
    
if __name__ == "__main__":
    data = s3.get_data(bucket_name="ads-recommender-system", file_path="/airflow_subprocess_data/product_process_data.csv")
    
    topproduct = Topproduct(data)
    data = topproduct.top_20()
    
    s3.post_data(bucket_name="ads-recommender-system", file_path="/airflow_subprocess_data/topproducts_data.csv", data = data)