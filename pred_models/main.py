import logging

from pred_models import logger, bq_client, upload_file_to_gcs, load_parquet_bq
from google.cloud import bigquery
import os
from time import sleep
from enum import Enum
from random import randrange
import cProfile
import pstats


class Disposition(Enum):
    TRUNCATE = 1
    APPEND = 2


def upload_df_to_cloud(df, table_name, write_disposition):
    data_folder = '../data/pred_models'
    bucket_name = 'dbtdemo'
    project_name = 'dbt-demo-422300'
    database_name = 'dbt_pk'

    parquet_path = os.path.join(data_folder, f'{table_name}.parquet')
    gcs_blob_name = os.path.basename(parquet_path)

    table_id = f"{project_name}.{database_name}.{table_name}"
    gcs_uri = f"gs://{bucket_name}/{gcs_blob_name}"

    logger.debug(f'Loading to cloud')
    df.to_parquet(parquet_path, engine='fastparquet')
    upload_file_to_gcs(parquet_path, bucket_name, gcs_blob_name)
    load_parquet_bq(gcs_uri, table_id, write_disposition.name)

    logger.info(f"Load to cloud complete: {table_name}")


def run_models(static, events, disposition):
    """
    Choose a completely random song from the song table.
    """
    table_name = 'model_recommendations'

    df = events.event_df.copy()

    description = "Choose a completely random song from the song table."
    logger.debug(f'Making Model A recommendations: {description}')
    df['model_a_song_id'] = df['song_id'].apply(lambda x: get_random_song(static, x))

    description = "Choose a song with the same artist."
    logger.debug(f'Making Model B recommendations: {description}')
    df['model_b_song_id'] = df['song_id'].apply(lambda x: get_song_same_artist(static, x))

    description = """
        Choose a song with an artist who is described being similar to the current artist.
        If the artist does not have similar artists defined, recommend a song by the same artist.
        """
    logger.debug(f'Making Model C recommendations: {description}')
    df['model_c_song_id'] = df['song_id'].apply(lambda x: get_song_similar_artist(static, x))

    # These model results look too similar to Model B
    # description = """
    #         Choose a song with an artist who is described using the same genre as the current artist.
    #         If the artist does not have a genre, recommend a song by the same artist.
    #     """
    # logger.debug(f'Making Model D recommendations: {description}')
    # df['model_d_song_id'] = df['song_id'].apply(lambda x: get_song_same_genre(static, x))

    df = df.drop(['artist_id', 'song_id'], axis=1)

    upload_df_to_cloud(df, table_name, disposition)
    return df

def get_random_song(static, song_id):
    num_songs = len(static.song_df)
    index = randrange(1, num_songs) - 1
    return static.song_df.iloc[index].song_id

def get_song_same_artist(static, song_id):
    artist_id = static.artist_lookup[song_id]
    songs = static.same_artist_lookup[artist_id]
    return songs[randrange(0, len(songs))]

def get_song_similar_artist(static, song_id):
    artist_id = static.artist_lookup[song_id]
    try:
        artists = static.similar_lookup[artist_id]
    except KeyError:
        # Not all artist have genre tags defined. Pick a random song from same artist
        return get_song_same_artist(static, song_id)
    artist = artists[randrange(0, len(artists))]
    songs = static.same_artist_lookup[artist]
    return songs[randrange(0, len(songs))]


def get_song_same_genre(static, song_id):
    artist_id = static.artist_lookup[song_id]
    try:
        genres = static.artist_genres[artist_id]
    except KeyError:
        # Not all artist have genre tags defined. Pick a random song from same artist
        return get_song_same_artist(static, song_id)
    genre = genres[randrange(0, len(genres))]
    artists = static.genre_lookup[genre]
    artist = artists[randrange(0, len(artists))]
    songs = static.same_artist_lookup[artist]
    return songs[randrange(0, len(songs))]


class StaticData():
    def __init__(self):
        logger.info('Importing static data')

        logger.debug('Importing songs')
        query = """ SELECT song_id, artist_id FROM dbt_pk.song"""
        self.song_df = bq_client.query(query).to_dataframe()

        logger.debug('Importing artist songs')
        self.artist_lookup = {} # given song_id, find artist_id
        self.same_artist_lookup = {} # given artist_id, find list of all their song_ids
        for index, row in self.song_df.iterrows():
            song_id = row.song_id
            artist_id = row.artist_id
            if artist_id not in self.same_artist_lookup:
                self.same_artist_lookup[artist_id] = [song_id]
            else:
                self.same_artist_lookup[artist_id].append(song_id)
            self.artist_lookup[song_id] = artist_id

        logger.debug('Importing similarity')
        query = """ SELECT target, similar FROM dbt_pk.similarity 
                    WHERE target IN (SELECT DISTINCT(artist_id) FROM dbt_pk.event)
                    AND similar IN (SELECT DISTINCT(artist_id) FROM dbt_pk.event) """
        similar_df = bq_client.query(query).to_dataframe()
        logger.debug(f'{len(similar_df)} rows.')
        self.similar_lookup = {} # given artist_id, find list of all similar artist_ids
        for index, row in similar_df.iterrows():
            target = row.target
            similar = row.similar
            if target not in self.similar_lookup:
                self.similar_lookup[target] = [similar]
            else:
                self.similar_lookup[target].append(similar)
            if similar not in self.similar_lookup:
                self.similar_lookup[similar] = [target]
            else:
                self.similar_lookup[similar].append(target)
        # Remove duplicates
        for key, value in self.similar_lookup.items():
            self.similar_lookup[key] = list(set(value))

        # THIS CODE USED FOR MODEL D WHICH IS CURRENTLY INACTIVE
        # logger.debug('Importing mbtags')
        # query = """ SELECT artist_id, mbtag FROM dbt_pk.artist_mbtag """
        # mbtag_df = bq_client.query(query).to_dataframe()
        # self.genre_lookup = {} # given genre (mbtag), find list of all artist who use that genre
        # self.artist_genres = {} # given artist_id, find list of all genres that artist uses
        # for index, row in mbtag_df.iterrows():
        #     genre = row.mbtag
        #     artist_id = row.artist_id
        #     if artist_id not in self.artist_lookup.keys():
        #         continue
        #     if genre not in self.genre_lookup:
        #         self.genre_lookup[genre] = [artist_id]
        #     else:
        #         self.genre_lookup[genre].append(artist_id)
        #     if artist_id not in self.artist_genres:
        #         self.artist_genres[artist_id] = [genre]
        #     else:
        #         self.artist_genres[artist_id].append(genre)


class EventData():
    def __init__(self):
        self.event_df = None

    def get_data(self, last_event_id):
        logger.info('Importing events')
        self.event_df = None
        query = f""" SELECT event_id, userId, ts, song_id, artist_id 
                    FROM dbt_pk.event 
                    WHERE event_id > @last_event_id
                """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("last_event_id", "INT64", last_event_id),
            ]
        )
        self.event_df = bq_client.query(query, job_config=job_config).to_dataframe()
        logger.debug(f'Imported {len(self.event_df)} events')


def main():
    logger.setLevel(level=logging.INFO)

    logger.info('Starting PredictiveModels')
    static = StaticData()
    events = EventData()

    # The first iteration will start from scratch, and clear out any previous recommendations
    disposition = Disposition.TRUNCATE
    last_event_id = -1

    while True:
        events.get_data(last_event_id)
        if len(events.event_df) > 0:
            last_event_id = int(events.event_df['event_id'].max())
            run_models(static, events, disposition)

            # All further iterations will append new event recommendations
            disposition = Disposition.APPEND
            logger.info("Completed model runs. Waiting for new data...")
        else:
            logger.info("No new events. Waiting for new data...")
        sleep(10)

if __name__ == "__main__":
    profile = False

    if profile:
        with cProfile.Profile() as profile:
            main()

        stats = pstats.Stats(profile).sort_stats("cumtime")
        stats.print_stats(r"\((?!\_).*\)$")  # Exclude private and magic callables.
    else:
        main()





