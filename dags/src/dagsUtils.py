import pandas as pd
def filtrar_advertiser(raw_data, active_advertisers):
    return raw_data[raw_data["advertiser_id"].isin(active_advertisers.advertiser_id)]