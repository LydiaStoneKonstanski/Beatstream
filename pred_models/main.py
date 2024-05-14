import logging
import time

import pandas as pd
from pred_models import logger, bq_client, upload_file_to_gcs, load_parquet_bq
import os
from time import sleep
from enum import Enum
from random import randrange
import cProfile
import pstats
from datetime import datetime


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


def run_model_a(static, events, disposition):
    """
    Choose a completely random song from the song table.
    """
    table_name = 'model_a'

    logger.debug(f'Starting {table_name}')
    df = events.event_df.copy()

    df['rec_song_id'] = df['song_id'].apply(lambda x: get_random_song(static, x))
    df = df.drop(['artist_id', 'song_id'], axis=1)

    upload_df_to_cloud(df, table_name, disposition)
    return df

def run_model_b(static, events, disposition):
    """
    Choose a song with the same artist.
    """
    table_name = 'model_b'

    logger.debug(f'Starting {table_name}')
    df = events.event_df.copy()

    df['rec_song_id'] = df['song_id'].apply(lambda x: get_song_same_artist(static, x))
    df = df.drop(['artist_id', 'song_id'], axis=1)

    upload_df_to_cloud(df, table_name, disposition)
    return df


def run_model_c(static, events, disposition):
    """
    Choose a song with an artist who is described being similar to the current artist.
    If the artist does not have similar artists defined, recommend a song by the same artist.
    """
    table_name = 'model_c'

    logger.debug(f'Starting {table_name}')
    df = events.event_df.copy()

    df['rec_song_id'] = df['song_id'].apply(lambda x: get_song_similar_artist(static, x))
    df = df.drop(['artist_id', 'song_id'], axis=1)

    upload_df_to_cloud(df, table_name, disposition)
    return df

def run_model_d(static, events, disposition):
    """
    Choose a song with an artist who is described using the same genre as the current artist.
    If the artist does not have a genre, recommend a song by the same artist.
    """
    table_name = 'model_d'

    logger.debug(f'Starting {table_name}')
    df = events.event_df.copy()

    df['rec_song_id'] = df['song_id'].apply(lambda x: get_song_same_genre(static, x))
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


def score_model(model_df, static, events, table_name, disposition):
    logger.info(f'Computing score for {table_name}')
    df = model_df.copy()

    # Splitting into smaller tables for performance
    user_events_dfs = {}
    for userId in static.users:
        user_events_dfs[userId] = events.event_df[events.event_df['userId'] == userId]

    df['score'] = df.apply(lambda x: get_score(x['ts'], x['rec_song_id'], user_events_dfs[x['userId']]), axis=1)
    df = df.drop(['rec_song_id'], axis=1)
    upload_df_to_cloud(df, table_name, disposition)

    logger.debug(f'Computing cumulative score for {table_name}')
    df['date'] = df.apply(lambda x: get_date_from_ts(x['ts']), axis=1)
    cumulative_df = df.groupby(['date'], as_index=False).sum()

    cumulative_df = cumulative_df.drop(['userId', 'ts'], axis=1)
    cumulative_df['cumulative_score'] = cumulative_df.apply(lambda x: get_cumulative_score(x['date'], cumulative_df), axis=1)
    cumulative_df['ts'] = cumulative_df.apply(lambda x: get_ts_from_date(x['date']), axis=1)
    cumulative_df = cumulative_df.drop(['date'], axis=1)
    upload_df_to_cloud(cumulative_df, table_name + '_cumulative', disposition)

def get_score(ts, song_id, df):
    """
    A recommendation gets one point for each time the user plays the song after the recommendation is made.
    This means the earlier you can find the user's new favorite song, the higher score you get.
    """
    score = len(df[(df['song_id'] == song_id) & (df['ts'] > ts)])
    return score

def get_cumulative_score(date, df):
    cumulative_score = df[(df['date'] <= date)]['score'].sum()
    return cumulative_score

def get_date_from_ts(ts):
    return datetime.fromtimestamp(ts / 1000.0).date()

def get_ts_from_date(date):
    return time.mktime(date.timetuple()) * 1000

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

        logger.debug('Importing users')
        query = """ select userId from dbt_pk.user """
        self.users = bq_client.query(query).to_dataframe()['userId'].tolist()


class EventData():
    def __init__(self):
        self.event_df = None

    def get_data(self):
        logger.info('Importing events')
        query = """ SELECT userId, ts, song_id, artist_id FROM dbt_pk.static_events_5_12_24 """
        self.event_df = bq_client.query(query).to_dataframe()
        logger.debug(f'Imported {len(self.event_df)} events')


def main():
    logger.setLevel(level=logging.INFO)

    logger.info('Starting PredictiveModels')
    static = StaticData()
    events = EventData()
    disposition = Disposition.TRUNCATE

    while True:
        events.get_data()

        model_df = run_model_a(static, events, disposition)
        score_model(model_df, static, events, 'model_a_score', disposition)

        model_df = run_model_b(static, events, disposition)
        score_model(model_df, static, events, 'model_b_score', disposition)

        model_df = run_model_c(static, events, disposition)
        score_model(model_df, static, events, 'model_c_score', disposition)

        # model_df = run_model_d(static, events, disposition)
        # score_model(model_df, static, events, 'model_d_score', disposition)

        logger.info("Completed model runs. Waiting for new data...")
        break
        sleep(60)

if __name__ == "__main__":
    profile = False

    if profile:
        with cProfile.Profile() as profile:
            main()

        stats = pstats.Stats(profile).sort_stats("cumtime")
        stats.print_stats(r"\((?!\_).*\)$")  # Exclude private and magic callables.
    else:
        main()





