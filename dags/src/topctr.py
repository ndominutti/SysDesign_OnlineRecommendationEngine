import pandas as pd
import .S3utils as s3


class Topctr:
    def __init__(self, data: pd.DataFrame):
        """Model that performs recommendation on last day ads CTR

        Args:
            data (pd.DataFrame): dataframe where to perform the recommendation
        """
        self.data = data

    def top_20(self, execution_date: str) -> pd.DataFrame:
        """Perform recommendation returning top20 ads with best CTR for
        last day for advertiser

        Args:
            execution_date (str): excecution_date from airflow dag

        Returns:
            pd.DataFrame: dataframe with top20 products with best CTR for
        last day for advertiser
        """
        self.data = self.data[self.data["date"] == execution_date]
        self.data["clicked"] = self.data["type"].apply(lambda x: x == "click")

        CTR_index = pd.DataFrame(
            self.data.groupby(["advertiser_id", "product_id"])["clicked"].mean()
        )

        CTR_index = CTR_index.reset_index().sort_values(
            by=["advertiser_id", "clicked"], ascending=False
        )

        CTR_index = CTR_index.rename(columns={"clicked": "CTR"})

        return CTR_index.groupby(["advertiser_id"]).head(20)
