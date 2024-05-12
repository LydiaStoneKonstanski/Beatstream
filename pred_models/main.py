import pandas as pd
from pred_models import logger, bq_client, upload_file_to_gcs, load_parquet_bq
import os
from time import sleep
from enum import Enum
from random import randrange
import cProfile
import pstats


class Disposition(Enum):
    TRUNCATE = 1
    APPEND = 2


class PredictiveModel:
    def __init__(self, table_name, table_description, df):
        self.table_name = table_name
        self.description = table_description
        self.df = df

    def upload_df_to_cloud(self, write_disposition):
        data_folder = '../data/pred_models'
        bucket_name = 'dbtdemo'
        project_name = 'dbt-demo-422300'
        database_name = 'dbt_pk'

        parquet_path = os.path.join(data_folder, f'{self.table_name}.parquet')
        gcs_blob_name = os.path.basename(parquet_path)

        table_id = f"{project_name}.{database_name}.{self.table_name}"
        gcs_uri = f"gs://{bucket_name}/{gcs_blob_name}"

        logger.debug(f'Loading to cloud')
        self.df.to_parquet(parquet_path, engine='fastparquet')
        upload_file_to_gcs(parquet_path, bucket_name, gcs_blob_name)
        load_parquet_bq(gcs_uri, table_id, write_disposition.name)

        logger.info(f"Load to cloud complete: {self.table_name}")


def run_model_a(static, events, disposition):
    table_name = 'model_a'
    table_description = """
            Choose a completely random song from the song table.
            """

    logger.debug(f'Starting {table_name}')
    df = events.event_df.copy()

    df['rec_song_id'] = df['song_id'].apply(lambda a: get_random_song(static, a))
    df = df.drop(['artist_id', 'song_id'], axis=1)

    model_a = PredictiveModel(table_name, table_description, df)
    model_a.upload_df_to_cloud(disposition)
    return model_a

def get_random_song(static, song_id):
    num_songs = len(static.song_df)
    index = randrange(1, num_songs) - 1
    return static.song_df.iloc[index].song_id


def score_model(events, model):
    df = model.df.copy()
    df['song_id'] = events.event_df['song_id'].to_numpy()
    #df['count'] = df.groupby((df['rec_song_id']==df['song_id']))

def get_score():
    pass

class StaticData():
    def __init__(self):
        logger.info('Importing static data')

        logger.debug('Importing songs')
        query = """ SELECT song_id, artist_id, year FROM dbt_pk.song"""
        self.song_df = bq_client.query(query).to_dataframe()

        # logger.debug('Importing similarity')
        # # TODO: This query is slow, probably because it is connecting to raw table.
        # # Migrate to similarity table once exists
        # query = """ SELECT target, similar FROM dbt_pk.similarity_raw """
        # similar_df = bq_client.query(query).to_dataframe()
        # self.similar_lookup = {}
        # for index, row in similar_df.iterrows():
        #     target = row.target
        #     similar = row.similar
        #     if target not in self.similar_lookup:
        #         self.similar_lookup[target] = [similar]
        #     else:
        #         self.similar_lookup[target].append(similar)
        #     if similar not in self.similar_lookup:
        #         self.similar_lookup[similar] = [target]
        #     else:
        #         self.similar_lookup[similar].append(target)
        # # Remove duplicates
        # for key, value in self.similar_lookup.items():
        #     self.similar_lookup[key] = list(set(value))

        logger.debug('Importing mbtags')
        query = """ SELECT artist_id, mbtag FROM dbt_pk.artist_mbtag """
        mbtag_df = bq_client.query(query).to_dataframe()
        self.genre_lookup = {}
        for index, row in mbtag_df.iterrows():
            genre = row.mbtag
            artist_id = row.artist_id
            if genre not in self.genre_lookup:
                self.genre_lookup[genre] = [artist_id]
            else:
                self.genre_lookup[genre].append(artist_id)

        # logger.info(
        #     f'Loaded {len(self.song_df)} songs, {len(similar_df)} artist similarities, and {len(mbtag_df)} genre tags.')


class EventData():
    def __init__(self):
        self.event_df = None

    def get_data(self):
        logger.info('Importing events')
        query = """ SELECT userId, ts, song_id, artist_id FROM dbt_pk.event """
        self.event_df = bq_client.query(query).to_dataframe()
        logger.debug(f'Imported {len(self.event_df)} events')


def main():
    logger.info('Starting PredictiveModels')
    static = StaticData()
    events = EventData()
    disposition = Disposition.TRUNCATE

    while True:
        events.get_data()

        run_model_a(static, events, disposition)

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





