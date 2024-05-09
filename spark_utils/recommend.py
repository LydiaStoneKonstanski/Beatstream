from sklearn.metrics.pairwise import cosine_similarity
from pyspark.sql import SparkSession
import pandas as pd
from topics import Topic


new_topic = Topic('listen-events', 9092)
spark = SparkSession.builder \
    .appName("data_prep") \
    .getOrCreate()
df = spark.read.parquet('/Users/chris/pyprojects/Beatstream/spark_utils/new_parqs/part-00000-eacaca76-0c15-4fd1-a0d7-9daf8a0012d8-c000.snappy.parquet')
pdf = df.toPandas()


# create user-item interaction matrix by getting the total amount of times a user
# has interacted with a song( implying that they like it)
# group that count by each user = pdf.groupby(['userId', 'song']) then .size() to get group of the userId with the size/count of the song
# users will be the rows and songs will be the columns
# achieve this with the unstack method that pivots the table while replacing any null values with zero
user_item_matrix = pdf.groupby(['userId', 'song']).size().unstack(fill_value=0)



# use the cosine similarity formula to find the similarities between users
# formula (A * B) / ||A|| * ||B||
# Using cosine similarity identifies other users who have similar listening patterns
cosine_sim = cosine_similarity(user_item_matrix)


# then we put the users into a user_similarity_matrix to cut down on the computational cost
# Precomputing and storing the similarities between users in a matrix makes it faster to generate recommendations
# storing the cosine similar users also allows us to use index-Based of similar users for any given user...
# rather than doing the math for every time we target a new user for recommendations.
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
    recommendations = weighted_scores.sort_values(ascending=False).head(10)

    return recommendations


user_id = 300
recommendations = recommend_songs(user_id, user_similarity_matrix, user_item_matrix)
print(f"Top recommended songs for user {user_id} are:\n{recommendations}")
