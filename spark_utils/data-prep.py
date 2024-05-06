import os
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
from pyspark.sql.functions import col, from_json
import featuretools as ft
# pip3 install --upgrade setuptools     in order to use featuretools
from topics import Topic


new_topic = Topic('listen-events', 9092)


spark = SparkSession.builder \
    .appName("data_prep") \
    .getOrCreate()

schema = StructType([
    StructField("artist", StringType(), True),
    StructField("song", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("ts", LongType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("userId", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("userAgent", StringType(), True),
    StructField("level", StringType(), True),
    StructField("registration", LongType(), True),
])


df = spark.read.json("/Users/chris/Downloads/sample/listen_events.json", schema=schema)

# df.createOrReplaceTempView("events")
#
# spark.sql("""
#     SELECT *
#     FROM events
# """).show()


pdf = df.toPandas()
pd.set_option('display.max_columns', None)
# Correctly convert UNIX timestamp to datetime
pdf['ts'] = pd.to_datetime(pdf['ts'], unit='ms')

# Prepare data for Featuretools
entity_set = ft.EntitySet(id='to_features')
entity_set = entity_set.add_dataframe(
    dataframe_name='events',
    dataframe=pdf,
    index='index'
)

feature_matrix, feature_defs = ft.dfs(entityset=entity_set, target_dataframe_name='events', max_depth=1)
print(feature_matrix.head())