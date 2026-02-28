import duckdb
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
con = duckdb.connect("analytics/analytics.duckdb")

user_result = con.execute("""
SELECT *
FROM read_parquet(               
'/Users/hrutika/Downloads/logs/iceberg_warehouse/user_daily_summary/data/**/*.parquet'
)
LIMIT 5
""").fetchdf()

print(user_result)
print("---------------------------------------------------------------------------------")
tracks_result = con.execute("""
SELECT *
FROM read_parquet(               
'/Users/hrutika/Downloads/logs/iceberg_warehouse/dim_tracks/data/**/*.parquet'
)
LIMIT 5
""").fetchdf()
print(tracks_result)





