import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
from pyspark.sql.functions import col, from_json, current_timestamp
from topics import Topic

# this line of code is how we deploy are spark app
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark.py


new_topic = Topic('listen-events', 9092)


# a lot like flask. this is where are app is created when we run the program

# allocate 2GB of memory for each execution to make sure the app doesn't fail
# while processing 1.2GB of data

# assign 2 cores to the app allowing us to run up to 2 tasks at a time


# determines the amount of spark partitions to use (the defualt is 200)
# 1.2 million rows of 1 kb rows
# 1.2 GB or 1200MB
# each partition will handle about 6 MB

# .config("spark.dynamicAllocation.enabled", "true") \
#     .config("spark.dynamicAllocation.minExecutors", "1") \
#     .config("spark.dynamicAllocation.maxExecutors", "25") \
#     .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
#     .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \

spark = SparkSession.builder \
    .appName("beat_streamer") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", '4g') \
    .config("spark.executor.cores", "1") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()


# create your schema based on the keys in the json
# much like how we create schema in sqlalchemy
schema1 = StructType([
    StructField('event_id', IntegerType(),False),
    StructField("ts", LongType(), True),
    StructField("city", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("state", StringType(), True),
    StructField("userId", IntegerType(), True),
    StructField("artist_id", IntegerType(), True),
    StructField("song_id", IntegerType(), True)
])


# below is the spark equivalent of a consumer
# the max offsets determines how much of the data will be received per stream
# setting this to 20000 should ensure that the full amount of 1.2GB
# should be received within 1-2min(s)
# you can also look for patterns in all topics! .option("subscribePattern", "topic.*") \
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"localhost:{new_topic.port}") \
    .option("subscribe", f"{new_topic.topic}") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 4000) \
    .load() \
    .select(from_json(col("value").cast("string"), schema1).alias("data")) \
    .select("data.*")


# spark.sparkContext.setLogLevel("WARN")
# query = df \
#     .coalesce(1) \
#     .writeStream \
#     .format("parquet") \
#     .trigger(processingTime="1 minute")\
#     .trigger(once=True) \
#     .option("path", "/Users/chris/pyprojects/Beatstream/data/users") \
#     .option("checkpointLocation", "/Users/chris/pyprojects/Beatstream/data/users-warehouse") \
#     .start()
spark.sparkContext.setLogLevel("WARN")
query = df \
    .coalesce(1) \
    .writeStream \
    .format("parquet") \
    .option("path", "/Users/chris/pyprojects/Beatstream/data/users") \
    .option("checkpointLocation", "/Users/chris/pyprojects/Beatstream/data/users-warehouse") \
    .trigger(processingTime="1 minute") \
    .outputMode("append") \
    .start()

query.awaitTermination()
query.stop()
spark.stop()



# df2 = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", f"localhost:{new_topic.port}") \
#     .option("subscribe", "analysis") \
#     .option("startingOffsets", "earliest") \
#     .option("maxOffsetsPerTrigger", 10000) \
#     .load() \
#     .select(from_json(col("value").cast("string"), schema2).alias("data2")) \
#     .select("data2.*")
#
#
# df3 = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", f"localhost:{new_topic.port}") \
#     .option("subscribe", "tracks") \
#     .option("startingOffsets", "earliest") \
#     .option("maxOffsetsPerTrigger", 10000) \
#     .load() \
#     .select(from_json(col("value").cast("string"), schema3).alias("data3")) \
#     .select("data3.*")

# df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
# # parse the json and use the schema
#
# parsed_df = df.withColumn("data", from_json(col("value"), schema))
# found a way to use the SQL commands we all know and love
# df = df.withColumn("event_time", current_timestamp())
# df = df.withWatermark("event_time", "10 minutes")
#
# df2 = df2.withColumn("event_time", current_timestamp())
# df2 = df2.withWatermark("event_time", "10 minutes")
#
# df3 = df3.withColumn("event_time", current_timestamp())
# df3 = df3.withWatermark("event_time", "10 minutes")




# df2 = df2.withColumnRenamed('artist_name', 'artist')
# df3 = df3.withColumnRenamed('artist_name', 'artist')

# df.createOrReplaceTempView("events")
# df2.createOrReplaceTempView("tracks_details")
# df3.createOrReplaceTempView("tracks_artists")


# tracks = spark.sql("""
# SELECT e.*,
#     regexp_replace(t.artist, '^b\\\\''|''$', '') AS cleaned_artist,
#     regexp_replace(t.track_id, '^b\\\\''|''$', '') AS cleaned_track_id
# FROM events e
# LEFT OUTER JOIN tracks t
# ON e.artist = t.artist
# """)
# ok
# tracks = spark.sql("""
# SELECT
#        regexp_replace(artist, "^b['\\"]|['\\"]$", '') AS artist,
#        regexp_replace(track_id, "^b\\\\'|'$", '') AS track_id
# FROM track_artists
# """)



# result_df = spark.sql("""
# SELECT td.time_signature, td.tempo, td.mode, td.loudness, td.key, ta.artist
# FROM tracks_artists ta
# JOIN tracks_details td ON ta.track_id = td.track_id
# """)


# combined_query = spark.sql("""
# WITH CleanedTracks AS (
#     SELECT
#         regexp_replace(artist, "^b['\\"]|['\\"]$", '') AS artist,
#         regexp_replace(track_id, "^b\\\\'|'$", '') AS track_id
#     FROM tracks_artists
# )
# SELECT td.time_signature, td.tempo, td.mode, td.loudness, td.key, ct.artist
# FROM CleanedTracks ct
# JOIN tracks_details td ON ct.track_id = td.track_id
# """)

# combined_clean_query = spark.sql("""
# WITH cleaned_tracks AS (
#     SELECT
#         regexp_replace(artist, "^b['\\"]|['\\"]$", '') AS artist,
#         regexp_replace(track_id, "^b\\\\'|'$", '') AS cleaned_track_id
#     FROM tracks_artists
# ),
# cleaned_details AS (
#     SELECT
#         time_signature, tempo, mode, loudness, key,
#         regexp_replace(track_id, "^b\\\\'|'$", '') AS cleaned_track_id
#     FROM tracks_details
# )
# SELECT
#     cd.time_signature, cd.tempo, cd.mode, cd.loudness, cd.key, ct.artist
# FROM cleaned_tracks ct
# JOIN cleaned_details cd ON ct.cleaned_track_id = cd.cleaned_track_id
# """)



# combined_and_events_query = spark.sql("""
# WITH cleaned_tracks AS (
#     SELECT
#         regexp_replace(artist, "^b['\\"]|['\\"]$", '') AS artist,
#         regexp_replace(track_id, "^b\\\\'|'$", '') AS track_id,
#         regexp_replace(artist_id, "^b\\\\'|'$", '') AS cleaned_artist_id
#     FROM tracks_artists
# ),
# cleaned_details AS (
#     SELECT
#         time_signature, tempo, loudness,
#         regexp_replace(track_id, "^b\\\\'|'$", '') AS cleaned_track_id
#     FROM tracks_details
# ),
# cleaned_events AS (
#     SELECT
#         song, FROM_UNIXTIME(ts) date, city, zip, state, userId, gender, duration, firstname, lastname,
#         regexp_replace(artist, "^b['\\"]|['\\"]$", '') AS cleaned_artist
#     FROM events
# )
# SELECT
#     e.song, e.date, e.city, e.zip, e.state, e.userId, e.gender, e.duration, e.firstname, e.lastname,
#     e.cleaned_artist artist, cd.time_signature, cd.tempo, cd.loudness, ct.cleaned_artist_id artist_id, ct.track_id
# FROM cleaned_events e
# JOIN cleaned_tracks ct ON e.cleaned_artist = ct.artist
# JOIN cleaned_details cd ON ct.track_id = cd.cleaned_track_id
# """)
#
#
# combined_and_events_query.createOrReplaceTempView('combined')
#
#
# user = spark.sql("""
#     SELECT userId userID, firstname, lastname
#     FROM combined
# """)
#
#
# artist = spark.sql("""
#     SELECT artist_id artistID, artist
#     FROM combined
# """)
#
#
# song = spark.sql("""
#     SELECT track_id songID, song, tempo, time_signature, loudness, artist_id
#     FROM combined
# """)

# event = spark.sql("""
#     SELECT date, state, city, zip, track_id song_id, userId user_id
#     FROM combined
# """)




# spark.sparkContext.setLogLevel("WARN")
# query1 = user \
#     .coalesce(1) \
#     .writeStream \
#     .format("parquet") \
#     .option("path", "/Users/chris/pyprojects/Beatstream/data/users") \
#     .option("checkpointLocation", "/Users/chris/pyprojects/Beatstream/data/users-warehouse") \
#     .start()
#
# spark.sparkContext.setLogLevel("WARN")
# query2 = artist \
#     .coalesce(1) \
#     .writeStream \
#     .format("parquet") \
#     .option("path", "/Users/chris/pyprojects/Beatstream/data/artists") \
#     .option("checkpointLocation", "/Users/chris/pyprojects/Beatstream/data/artists-warehouse") \
#     .start()
#
# spark.sparkContext.setLogLevel("WARN")
# query3 = song \
#     .coalesce(1) \
#     .writeStream \
#     .format("parquet") \
#     .option("path", "/Users/chris/pyprojects/Beatstream/data/songs") \
#     .option("checkpointLocation", "/Users/chris/pyprojects/Beatstream/data/songs-warehouse") \
#     .start()

# spark.sparkContext.setLogLevel("WARN")
# query4 = event \
#     .coalesce(1) \
#     .writeStream \
#     .format("parquet") \
#     .option("path", "/Users/chris/pyprojects/Beatstream/data/events") \
#     .option("checkpointLocation", "/Users/chris/pyprojects/Beatstream/data/events-warehouse") \
#     .trigger(processingTime='5 minutes') \
#     .start()




#
# query.awaitTermination()
# query.stop()
# spark.stop()
#query.awaitTermination()
# query2.awaitTermination()
# query3.awaitTermination()
# query4.awaitTermination()
# query1.stop()
# query2.stop()
# query3.stop()
# query4.stop()
# spark.stop()



# SELECT TRIM('#! ' FROM '    #SQL Tutorial!    ') AS TrimmedString;
#
#
# # spark.sql("""
# # SELECT *, TRIM(, ) , tracks.*
# # FROM events
# # JOIN analysis.artist
# # """)




# spark.sparkContext.setLogLevel("WARN")
# query = artist \
#     .writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()
# query.awaitTermination()
# query.stop()
# spark.stop()
#


# Use SQL to format the timestamp and create a new DataFrame
# try counting the number of times an artist/song appears in the listen-events


# top_10_songs = spark.sql("""
#     SELECT
#     song,
#     COUNT(song) AS song_count,
#     MAX(DATE(FROM_UNIXTIME(ts / 1000))) AS last_played_date
#     FROM Hits
#     GROUP BY song
#     ORDER BY song_count DESC
#     LIMIT 10
# """)
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


# set the log level to avoid getting too many info-level logs every for every execution.



