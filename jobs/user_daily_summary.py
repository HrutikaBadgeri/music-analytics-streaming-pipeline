from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, countDistinct, when, col, lit
#from pyspark.types import *
from datetime import date, timedelta

spark = SparkSession.builder \
        .appName("userDailySummary") \
        .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type","hadoop") \
        .config("spark.sql.catalog.local.warehouse","/Users/hrutika/Downloads/logs/iceberg_warehouse") \
        .getOrCreate()


spark.sparkContext.setLogLevel("WARN")
yesterday = date.today() - timedelta(days=1)
activity_df = spark.sql(f"""SELECT * FROM local.user_activity WHERE date = '2026-02-28'""")

daily_summary_df = activity_df.groupBy(
    "date",
    "user_id"
).agg(countDistinct("session_id").alias("sessions"),
    count("*").alias("total_events"),
    sum("ply").alias("plays"),
    sum("skp").alias("skips"),
    sum("lik").alias("likes"),
    sum("atp").alias("playlist_adds"),
    countDistinct(when(col("track_id").isNotNull(), col("track_id"))).alias("unique_tracks")
)

daily_summary_df.show()

daily_summary_df = daily_summary_df.withColumn("is_active",lit(1))

daily_summary_df = daily_summary_df.withColumn("multi_session_flag",when(col("sessions") > 1, 1).otherwise(0))
explorer_flag = when(col("unique_tracks") >= 5, 1).otherwise(0)
daily_summary_df = daily_summary_df.withColumn("explorer_flag",explorer_flag)
engaged_flag = when((col("plays") > 0) | (col("likes") > 0) | (col("playlist_adds") > 0),1).otherwise(0)
daily_summary_df = daily_summary_df.withColumn("heavy_engagement",engaged_flag)

daily_summary_df.show(truncate=True)

daily_summary_df.writeTo("local.user_daily_summary").append()

