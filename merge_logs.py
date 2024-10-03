import glob
import pandas as pd


logs = [pd.read_csv(filename, sep=" ", header=None) for filename in glob.glob("logs/*.log")]
df = pd.concat(logs, axis=0, ignore_index=True, sort=True)
df.columns = ["IP", "X1", "X2", "TIMESTAMP", "ROUTE", "ENDPOINT", "STATUS_CODE", "RESPONSE_TIME", "X3", "X4", "X5", "X6"]
df = df[["IP", "TIMESTAMP", "ENDPOINT", "STATUS_CODE", "RESPONSE_TIME"]]
df.TIMESTAMP = pd.to_datetime(df.TIMESTAMP.str[1:], errors="raise", format="%d/%b/%Y:%H:%M:%S")
df.sort_values(by="TIMESTAMP", ascending=True, inplace=True, ignore_index=True)
df.to_parquet(path="./logs/full_logs.parquet", engine="auto", compression="snappy")