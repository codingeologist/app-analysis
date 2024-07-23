import pandas as pd

df = pd.read_csv("visitor_locations.csv")
cols_to_keep = ["TIMESTAMP", "COUNTRY_NAME", "COUNTRY_CODE", "LATITUDE", "LONGITUDE"]

df = df[cols_to_keep]
df = df.groupby("COUNTRY_NAME").agg(
    COUNT=("COUNTRY_NAME", "size"),
    LATITUDE=("LATITUDE", "first"),
    LONGITUDE=("LONGITUDE", "first")
).reset_index()

df.to_csv("visitor_location_count.csv", index=False)