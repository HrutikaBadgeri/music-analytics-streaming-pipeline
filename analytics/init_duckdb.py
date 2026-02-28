import duckdb
con = duckdb.connect("analytics/analytics.duckdb")

# user_daily_summary view 
con.execute("""
CREATE OR REPLACE VIEW user_daily_summary AS
SELECT *
FROM read_parquet(
'/Users/hrutika/Downloads/logs/iceberg_warehouse/user_daily_summary/data/**/*.parquet'
)
""")

print("user_daily_summary view created")

# tracks view 
con.execute("""
CREATE OR REPLACE VIEW tracks_daily_summary AS
SELECT *
FROM read_parquet(
'/Users/hrutika/Downloads/logs/iceberg_warehouse/tracks_daily_summary/data/**/*.parquet'
)
""")
print("tracks_daily_summary view created")

# dim_users
con.execute("""
CREATE OR REPLACE VIEW dim_users AS
SELECT *
FROM read_parquet(
'/Users/hrutika/Downloads/logs/iceberg_warehouse/dim_users/data/**/*.parquet'
)
""")
print("dim_users view created")

# dim_tracks
con.execute("""
CREATE OR REPLACE VIEW dim_tracks AS
SELECT *
FROM read_parquet(
'/Users/hrutika/Downloads/logs/iceberg_warehouse/dim_tracks/data/**/*.parquet'
)
""")
print("dim_tracks view created")
