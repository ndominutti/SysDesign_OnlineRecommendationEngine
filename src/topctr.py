import pandas as pd
import S3utils as s3

class Topctr():
    def __init__(self, data):
        self.data = data
    def top_20(self):
        self.data["clicked"] = self.data["type"].apply(lambda x : x == "click")

        CTR_index = pd.DataFrame(self.data.groupby(["advertiser_id", "product_id"])["clicked"].mean())


        CTR_index = CTR_index.reset_index().sort_values(by=["advertiser_id","clicked"], ascending=False)

        return CTR_index.groupby(['advertiser_id']).head(20)

    
if __name__ == "__main__":
    data = s3.get_data(bucket_name="ads-recommender-system", file_path="/airflow_subprocess_data/ctr_process_data.csv")
    
    topctr = Topctr(data)
    data = topctr.top_20()
    
    s3.post_data(bucket_name="ads-recommender-system", file_path="/airflow_subprocess_data/topctr_data.csv", data = data)