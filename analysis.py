import time
import requests
import pandas as pd

df = pd.read_csv("logs.log", sep=" ", header=None)
df.columns = ["IP", "X1", "X2", "TIMESTAMP", "ROUTE", "ENDPOINT", "STATUS_CODE", "RESPONSE_TIME", "X3", "X4", "X5", "X6"]
df = df[["IP", "TIMESTAMP"]]
df.TIMESTAMP = pd.to_datetime(df.TIMESTAMP.str[1:], errors="raise", format="%d/%b/%Y:%H:%M:%S")

baseurl = "https://freeipapi.com/api/json/"

IP = []
countryname = []
countrycode = []
lat = []
lon = []
for index, row in df.iterrows():

    response = requests.get(url="{}{}".format(baseurl, row["IP"]))

    if response.status_code == 200:
        data = response.json()
        countryname.append(data["countryName"])
        countrycode.append(data["countryCode"])
        lat.append(data["latitude"])
        lon.append(data["longitude"])
        IP.append(row["IP"])
        print("index: {}, IP: {}, country: {}, code: {}".format(index, row["IP"], data["countryName"], data["countryCode"]))
        time.sleep(1)

df["COUNTRY_NAME"] = countryname
df["COUNTRY_CODE"] = countrycode
df["LATITUDE"] = lat
df["LONGITUDE"] = lon
df.to_csv("visitor_locations.csv", index=False)