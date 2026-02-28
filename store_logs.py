from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date, get_json_object, current_timestamp, when, to_timestamp
from pyspark.sql.types import *
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("rawEvents") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/Users/hrutika/Downloads/logs/iceberg_warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "events").option("startingOffsets", "latest").load()

raw_df = kafka_df.selectExpr(
    "CAST(value AS STRING) as raw_json",
    "timestamp as kafka_timestamp",
)

raw_df = raw_df.withColumn(
    "event_id",
    get_json_object(col("raw_json"), "$.event_id")
)

raw_df = raw_df.withColumn(
    "event_type",
    get_json_object(col("raw_json"), "$.event_type")
)

raw_df = raw_df.withColumn(
    "ingestion_time",
    to_timestamp(get_json_object(col("raw_json"), "$.ingestion_time"))
)
raw_df = raw_df.withColumn("date",to_date(col("ingestion_time")))
raw_df = raw_df.withColumn("processing_time",current_timestamp())

event_mapping = {
    "play": "ply",
    "skip": "skp",
    "like": "lik",
    "app_open": "ao",
    "app_close": "ac",
    "add_to_playlist": "atp"
}

event_df = raw_df

flag_columns = [
    when(col("event_type") == raw_event, 1)
    .otherwise(0)
    .cast(IntegerType())
    .alias(short_name)
    for raw_event, short_name in event_mapping.items()
]
event_df = raw_df.select("*", *flag_columns)

event_df = event_df.select("raw_json","kafka_timestamp","event_id","event_type","ingestion_time","date","processing_time","ply","skp","lik","ao","ac","atp")

query = event_df.writeStream \
    .outputMode("append") \
    .option("checkpointLocation", "/Users/hrutika/data_lake/checkpoints/raw") \
    .toTable("local.raw_events")


query.awaitTermination()