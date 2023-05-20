import pandas as pd
from . import S3utils as s3


class Topproduct:
    def __init__(self, data: pd.DataFrame):
        """Model that performs recommendation on last day shown products

        Args:
            data (pd.DataFrame): dataframe where to perform the recommendation
        """
        self.data = data

    def top_20(self, execution_date: str) -> pd.DataFrame:
        """Perform recommendation returning top20 products with more views for
        last day for advertiser

        Args:
            execution_date (str): excecution_date from airflow dag

        Returns:
            pd.DataFrame: dataframe with top20 products with more view for
        last day for advertiser
        """
        self.data = self.data[self.data["date"] == execution_date]
        self.data = (
            self.data.groupby(["advertiser_id", "product_id"])
            .size()
            .reset_index(name="event_count")
        )
        self.data = self.data.sort_values(
            ["advertiser_id", "event_count"], ascending=False
        )
        return self.data.groupby("advertiser_id").head(20)
