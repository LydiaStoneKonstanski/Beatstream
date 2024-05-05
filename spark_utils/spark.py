import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
from pyspark.sql.functions import col, from_json, from_unixtime
from topics import Topic

# this line of code is how we deploy are spark app
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark.py


new_topic = Topic('listen-events', 9092)

# a lot like flask. this is where are app is created when we run the program

# allocate 4 gigs of memory for each execution to make sure the app doesn't fail
# while processing a larger amount of data

# assign four cores to the app allowing us to run up to four tasks at a time

# determines the amount of spark partitions to use (the default is 200) which seemed too high for our current setup
# 1.2 million rows of 1 kb rows
# 1.2 GB or 1200MB
# each partition will handle about 12 MB

spark = SparkSession.builder \
    .appName("beat_streamer") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", '1g') \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "1") \
    .config("spark.dynamicAllocation.maxExecutors", "20") \
    .getOrCreate()

# this is the spark equivalent of a consumer
# you can also look for patterns in all topics! .option("subscribePattern", "topic.*") \
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"localhost:{new_topic.port}") \
    .option("subscribe", f"{new_topic.topic}") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 20000) \
    .load()
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# create your schema based on the keys in the json
# much like how we create schema in sqlalchemy
schema = StructType([
    StructField("artist", StringType(), True),
    StructField("song", StringType(), True),
    StructField("ts", LongType(), True),
    StructField("city", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("state", StringType(), True),
    StructField("userId", IntegerType(), True),
    # StructField("lastname", StringType(), True),
    # StructField("firstname", StringType(), True),
])

# parse the json and use the schema

parsed_df = df.withColumn("data", from_json(col("value"), schema))
# found a way to use the SQL commands we all know and love
parsed_df.createOrReplaceTempView("Hits")

# Use SQL to format the timestamp and create a new DataFrame
# try counting the number of times an artist/song appears in the listen-events


formatted_df = spark.sql("""
    SELECT 
    data.song AS song, 
    COUNT(data.song) AS song_count, 
    MAX(DATE(FROM_UNIXTIME(data.ts / 1000))) AS last_played_date
    FROM Hits
    GROUP BY data.song
    ORDER BY song_count DESC
    LIMIT 10
""")
#
#
# song_plays = parsed_df.select(
#     col('data.ts').alias('time'),
#     col("data.song").alias("song")
# ).groupBy("song").count() \
#     .orderBy(col('count') \
#     .desc()).limit(10)
#
# artist_listens = parsed_df.select(
#     col("data.artist").alias("artist")
# ).groupBy("artist").count()
#
# # how many users in each city?
#
# users_in_city = parsed_df.select(
#     col("data.city").alias("city")
# ).groupBy("city").count()

# use spark to create a new DataFrame from the kafka message
# set the log level to avoid getting too many info-level logs for every execution.
spark.sparkContext.setLogLevel("WARN")
query = formatted_df \
    .writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()
query.awaitTermination()
query.stop()
spark.stop()
