from pyspark.sql.types import *

user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("country", StringType(), True),
    StructField("subscription_plan", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("platform", StringType(), True)
])

content_schema = StructType([
    StructField("track_id", StringType(), True),
    StructField("artist_id", IntegerType(), True),
    StructField("album_id", IntegerType(), True),
    StructField("duration_ms", IntegerType(), True)
])
