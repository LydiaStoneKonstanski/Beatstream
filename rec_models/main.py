from rec_models import bq_client, upload_file_to_gcs, load_parquet_bq
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
from pyspark.sql.functions import col, from_json, current_timestamp
from spark_utils.topics import Topic

import os
from enum import Enum
import pandas as pd
#from spark_utils.recommend import recommend_songs
from sklearn.metrics.pairwise import cosine_similarity

class Disposition(Enum):
    TRUNCATE = 1
    APPEND = 2


# def recommend_song(user_id, user_similarity_matrix, user_item_matrix):
#     # Get similarity scores for the selected user with all other users
#     sim_scores = user_similarity_matrix.loc[user_id]
#
#     # Sort the similar users by similarity scores in descending order
#     # bringing all the most similar patterns to the top
#     sim_scores = sim_scores.sort_values(ascending=False)
#
#     # the most top/most similar user will of course be the user themselves
#     # so remember to skip iloc[0]
#     top_users = sim_scores.iloc[1:11].index
#
#     # Get the songs these similar users have interacted with
#     top_users_implied_ratings = user_item_matrix.loc[top_users]
#
#     # Calculate the weighted scores of songs based on user similarities and their interactions
#     # top_users_ratings.T transposes the DataFrame using the songs as the rows and the users as columns
#     # then we extract the similarity score into a numpy array
#     # then we get the dot product from the score of the top users
#     weighted_scores = top_users_implied_ratings.T.dot(sim_scores[top_users].values)
#
#     # Filter out songs the selected user has already interacted with
#     known_interactions = user_item_matrix.loc[user_id]
#     weighted_scores = weighted_scores[known_interactions == 0]
#
#     # Get the top song recommendations
#     recommendation = weighted_scores.sort_values(ascending=False).head(1)
#
#     return recommendation

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

# spark = SparkSession.builder \
#     .appName("beat_streamer") \
#     .config("spark.executor.memory", "4g") \
#     .config("spark.driver.memory", '4g') \
#     .config("spark.executor.cores", "1") \
#     .config("spark.sql.shuffle.partitions", "50") \
#     .getOrCreate()
#
#
# # create your schema based on the keys in the json
# # much like how we create schema in sqlalchemy
# schema1 = StructType([
#     StructField("ts", LongType(), True),
#     StructField("city", StringType(), True),
#     StructField("zip", StringType(), True),
#     StructField("state", StringType(), True),
#     StructField("userId", IntegerType(), True),
# ])
#
# df = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", f"localhost:{new_topic.port}") \
#     .option("subscribe", f"{new_topic.topic}") \
#     .option("startingOffsets", "earliest") \
#     .option("maxOffsetsPerTrigger", 4000) \
#     .load() \
#     .select(from_json(col("value").cast("string"), schema1).alias("data")) \
#     .select("data.*")
#
# spark.sparkContext.setLogLevel("WARN")
# query = df \
#     .coalesce(1) \
#     .writeStream \
#     .format("parquet") \
#     .option("path", "/Users/chris/pyprojects/Beatstream/data/users") \
#     .option("checkpointLocation", "/Users/chris/pyprojects/Beatstream/data/users-warehouse") \
#     .trigger(processingTime="1 minute") \
#     .outputMode("append") \
#     .start()
#
# query.awaitTermination()
# query.stop()
# spark.stop()


class PredictiveModel:
    def __init__(self, table_name, table_description, df):
        self.table_name = table_name
        self.description = table_description
        self.df = df

    def upload_df_to_cloud(self, write_disposition):
        data_folder = '../data/rec_models'
        bucket_name = 'dbtdemo'
        project_name = 'dbt-demo-422300'
        database_name = 'dbt_pk'

        parquet_path = os.path.join(data_folder, f'{self.table_name}.parquet')
        gcs_blob_name = os.path.basename(parquet_path)

        table_id = f"{project_name}.{database_name}.{self.table_name}"
        gcs_uri = f"gs://{bucket_name}/{gcs_blob_name}"

        self.df.to_parquet(parquet_path, engine='fastparquet')
        upload_file_to_gcs(parquet_path, bucket_name, gcs_blob_name)
        load_parquet_bq(gcs_uri, table_id, write_disposition.name)

        print(f"Load to cloud complete: {self.table_name}")

if __name__ == "__main__":

    table_name = 'rec_model'
    table_description = """
    """
    query = """ SELECT userId, song_id, DATE(TIMESTAMP_MILLIS(ts)) ts FROM dbt_pk.static_events_5_12_24"""
    pdf2 = pd.read_parquet('/Users/chris/pyprojects/Beatstream/spark_utils/new_parqs/7k_user_event test.parquet')
    whole_set = pd.read_parquet('/Users/chris/pyprojects/Beatstream/spark_utils/new_parqs/indexed_full_events.parquet')
    pdf = bq_client.query(query).to_dataframe()
    dropped_df = pdf.drop_duplicates(subset=["userId"], keep=False)
    latest_timestamps = dropped_df.groupby('userId')['ts'].agg('max').reset_index()
    user_list_with_timestamps = list(zip(latest_timestamps['userId'], latest_timestamps['ts']))
    # user_list = dropped_df['userId'].tolist()

    user_item_matrix = pdf.groupby(['userId', 'song_id']).size().unstack(fill_value=0)

    cosine_sim = cosine_similarity(user_item_matrix)

    user_similarity_matrix = pd.DataFrame(cosine_sim, index=user_item_matrix.index, columns=user_item_matrix.index)

    def recommend_songs(user_id, user_similarity_matrix, user_item_matrix):
        # Get similarity scores for the selected user with all other users
        sim_scores = user_similarity_matrix.loc[user_id]

        # Sort the similar users by similarity scores in descending order
        # bringing all the most similar patterns to the top
        sim_scores = sim_scores.sort_values(ascending=False)

        # the most top/most similar user will of course be the user themselves
        # so remember to skip iloc[0]
        top_users = sim_scores.iloc[1:11].index

        # Get the songs these similar users have interacted with
        top_users_implied_ratings = user_item_matrix.loc[top_users]

        # Calculate the weighted scores of songs based on user similarities and their interactions
        # top_users_ratings.T transposes the DataFrame using the songs as the rows and the users as columns
        # then we extract the similarity score into a numpy array
        # then we get the dot product from the score of the top users
        weighted_scores = top_users_implied_ratings.T.dot(sim_scores[top_users].values)

        # Filter out songs the selected user has already interacted with
        known_interactions = user_item_matrix.loc[user_id]
        weighted_scores = weighted_scores[known_interactions == 0]

        # Get the top song recommendations
        recommendation = weighted_scores.sort_values(ascending=False).head(10)

        return recommendation

    users = []
    songs = []
    stamps = []

    for user, ts in user_list_with_timestamps:
        recommendations = recommend_songs(user, user_similarity_matrix, user_item_matrix)
        # print(f"Top recommended song for user {user_id} is:\n{recommendations} at {ts}")
        users.append(user)
        songs.append(str(recommendations).split()[1:-2:2])
        stamps.append(ts)

    # max_ts = pdf['ts'].agg('max')
    # max_date = pd.to_datetime(max_ts, unit='ms')
    # min_ts = pdf['ts'].agg('min')
    # min_date = pd.to_datetime(min_ts, unit='ms')
    # print(f"\n\n\n\n\n\n\n{min_date} to {max_date}")
    # rec_dict = {'userId': users, 'song_id': songs, 'ts': stamps}
    # persisted = pd.DataFrame(rec_dict)



    score_dict = {}

    # Loop over each user and their respective timestamp and recommendation list
    for user, ts, recommended_songs in zip(users, stamps, songs):
        # Subset the PDF dataframe to only include the current user's data
        target_user = whole_set[whole_set['userId'] == user]

        # Calculate the score for the current user by counting how many recommended songs appear in their historical interactions
        score = sum(target_user['song_id'].isin(recommended_songs))
        score2 = []
        play_count = 0
        # for song in recommended_songs:
        #     score2.append(len(target_user[target_user['song_id'] == song]))
        #     print(score2)
        # Store the score in the dictionary using the user ID as the key
        score_dict[user] = score, (len(target_user))
        print(len(target_user))
    # Print the score dictionary to check the scores for each user
    print(score_dict)
    #print(pdf.size)
    # print(len(users))
    #print(songs)
    # print(pdf[pdf['userId'] == users[3]])
    # print(pdf[pdf['song_id'] == songs[3]])
    # filtered_df = pdf[(pdf['userId'] == users[0]) & (pdf['song_id'] == songs[0])]

    # to_table = pd.DataFrame(rec_dict)
    # print(pdf.groupby(['userId', 'song_id']).size().agg('max'))


    # model_a = PredictiveModel(table_name, table_description, to_table)
    # model_a.upload_df_to_cloud(Disposition.TRUNCATE)
    #
    # # pdf = df.toPandas()
    #
    # # create user-item interaction matrix by getting the total amount of times a user
    # # has interacted with a song( implying that they like it)
    # # group that count by each user = pdf.groupby(['userId', 'song']) then .size() to get group of the userId with the size/count of the song
    # # users will be the rows and songs will be the columns
    # # achieve this with the unstack method that pivots the table while replacing any null values with zero
    # user_item_matrix = pdf.groupby(['userId', 'song_id']).size().unstack(fill_value=0)
    #
    # # use the cosine similarity formula to find the similarities between users
    # # formula (A * B) / ||A|| * ||B||
    # # Using cosine similarity identifies other users who have similar listening patterns
    # cosine_sim = cosine_similarity(user_item_matrix)
    #
    # # then we put the users into a user_similarity_matrix to cut down on the computational cost
    # # Precomputing and storing the similarities between users in a matrix makes it faster to generate recommendations
    # # storing the cosine similar users also allows us to use index-Based of similar users for any given user...
    # # rather than doing the math for every time we target a new user for recommendations.
    # user_similarity_matrix = pd.DataFrame(cosine_sim, index=user_item_matrix.index, columns=user_item_matrix.index)
    # #dropped_df = pdf.drop_duplicates(subset=["userId"], keep=False)
    # # user_list = pdf['userId'].tolist()
    # #get the latest time stamp by sorting the dataframe then iterating through dropping the ones that are
    # #newer then latest for now
    # latest_time = 0
    # users = []
    # recs = []
    # scr = [90, 40, 80, 98]
    # sorted_df = pdf.sort_values(by='ts').reset_index(drop=True)
    #
    # for count in range(len(sorted_df)):
    #     current_row = sorted_df.loc[count]
    #     time_stamp = current_row['ts']
    #     current_user = 115
    #
    #     historical_data = sorted_df[sorted_df.ts < time_stamp]
    #     user_item_matrix = historical_data.groupby(['userId', 'song_id']).size().unstack(fill_value=0)
    #     print(user_item_matrix)
    #     #
    #     # if not user_item_matrix.empty:
    #     #     cosine_sim = cosine_similarity(user_item_matrix)
    #     #     user_similarity_matrix = pd.DataFrame(cosine_sim, index=user_item_matrix.index,columns=user_item_matrix.index)
    #     #
    #     #     # Check if the current user is in the user_similarity_matrix
    #     #     if current_user in user_similarity_matrix.index:
    #     #         recommendation = recommend_song(current_user, user_similarity_matrix, user_item_matrix)
    #     #         print(f"Top recommended song for user {current_user} is:\n{str(recommendation).split()[1]}")
    #     #     else:
    #     #         # Handle the case where the current user is not in the matrix
    #     #         print(f"No historical data available for user {current_user}")
    #     #         print(f"recommz")
    # # rec_dict = {'userId': users, 'song_id': recs}
    # # rec_df = pd.DataFrame(rec_dict)
    # # model_a = PredictiveModel(table_name, table_description, rec_df)
    # # model_a.upload_df_to_cloud(Disposition.TRUNCATE)