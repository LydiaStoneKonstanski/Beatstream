from pyspark.sql import SparkSession
from topic_utils.topics import Topic
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
from pyspark.sql.functions import col, from_json

# this line of code is how we deploy are spark app
# 'spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark.py'
def beat_spark(topic, port):
# a lot like flask. this is where are app is created when we run the program
    spark = SparkSession.builder \
        .appName("kafka_to_spark_example") \
        .getOrCreate()
    # this is the spark equivalent of a consumer
    # you can also look for patterns in all topics! .option("subscribePattern", "topic.*") \
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f"localhost:{port}") \
        .option("subscribe", f"{topic}") \
        .load()
    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    return df

