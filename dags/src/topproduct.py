import pandas as pd
from . import S3utils as s3


class Topproduct:
    def __init__(self, data):
        self.data = data

    def top_20(self):
        self.data = (
            self.data.groupby(["advertiser_id", "product_id"]).count().reset_index()
        )
        self.data = self.data.sort_values(["advertiser_id", "date"], ascending=False)
        return self.data.groupby("advertiser_id").head(20)
