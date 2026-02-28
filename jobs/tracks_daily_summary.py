from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, lit, when, countDistinct, col
from datetime import date, timedelta 

spark = SparkSession.builder \
        .appName("tracksDailySummary") \
        .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type","hadoop") \
        .config("spark.sql.catalog.local.warehouse","/Users/hrutika/Downloads/logs/iceberg_warehouse") \
        .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
yesterday = date.today() - timedelta(days=1)
activity_df = spark.sql(f"""SELECT * FROM local.user_activity WHERE date = '2026-02-28'""")
activity_df.show()
activity_df = activity_df.filter(col("track_id").isNotNull())
activity_df.show()
tracks_summary_df = activity_df.groupBy(
    "date",
    "track_id"
).agg(
    sum("ply").alias("plays"),
    sum("skp").alias("skips"),
    sum("lik").alias("likes"),
    sum("atp").alias("playlist_adds"),
    countDistinct("user_id").alias("unique_listeners")
)


tracks_summary_df = tracks_summary_df.withColumn("skip_rate",when(col("plays") > 0,col("skips") / col("plays")).otherwise(0))


tracks_summary_df = tracks_summary_df.withColumn("engagement_score", col("plays") + col("likes") + col("playlist_adds") - col("skips"))

tracks_summary_df = tracks_summary_df.withColumn("hit_score",(col("plays") * 1.0) + (col("likes") * 2.0) + (col("playlist_adds") * 3.0) -(col("skips") * 1.5))

tracks_summary_df.show(truncate=False)

tracks_summary_df.writeTo("local.tracks_daily_summary").append()
