import pandas as pd
from tqdm import tqdm


lviv = pd.read_csv("datasets/tg_parsed_alarms.csv")
ua = pd.read_csv("datasets/official_data_en.csv")


lviv["started"] = pd.to_datetime(lviv["started"])
lviv["ended"]   = pd.to_datetime(lviv["finished"])

ua["started"] = pd.to_datetime(ua["started_at"])
ua["ended"]   = pd.to_datetime(ua["finished_at"])

def find_regions_for_interval(start, end, ua_df):
    mask = (ua_df["started"] <= end) & (ua_df["ended"] >= start) & (ua_df["oblast"] != "Lvivska oblast")
    return sorted(ua_df.loc[mask, "oblast"].unique())

alarms_in_regions = []

for i, row in tqdm(lviv.iterrows(), total=len(lviv)):
    regions = find_regions_for_interval(row["started"], row["ended"], ua)
    alarms_in_regions.append("; ".join(regions))


lviv["alarms_in_regions"] = alarms_in_regions


lviv.to_csv("datasets/tg_parsed_alarms.csv", index=False)
