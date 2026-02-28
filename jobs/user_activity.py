from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, to_timestamp, to_date
from datetime import date, timedelta

spark = SparkSession.builder \
    .appName("userAnalytics") \
    .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type","hadoop") \
    .config("spark.sql.catalog.local.warehouse","/Users/hrutika/Downloads/logs/iceberg_warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# load yesterday's data
ydate = date.today() - timedelta(days=0)
raw_df = spark.sql(f"SELECT * from local.raw_events where date = '2026-02-28'")

user_df = raw_df.select(
    col("date"),
    col("event_type"),
    col("event_id"),
    col("ply"),
    col("skp"),
    col("lik"),
    col("atp"), 
    get_json_object("raw_json", "$.user_data.user_id").alias("user_id"),
    get_json_object("raw_json", "$.content_data.track_id").alias("track_id"),
    get_json_object("raw_json", "$.session_id").alias("session_id"),
    to_timestamp(get_json_object("raw_json", "$.client_event_time")).alias("client_time")
)
# user_df.show()
user_df.writeTo("local.user_activity").append()
