import pandas as pd
from . import S3utils as s3

class Topctr():
    def __init__(self, data):
        self.data = data
    def top_20(self):
        self.data["clicked"] = self.data["type"].apply(lambda x : x == "click")

        CTR_index = pd.DataFrame(self.data.groupby(["advertiser_id", "product_id"])["clicked"].mean())


        CTR_index = CTR_index.reset_index().sort_values(by=["advertiser_id","clicked"], ascending=False)

        return CTR_index.groupby(['advertiser_id']).head(20)

    def __call__(self):
        return self.top_20()
