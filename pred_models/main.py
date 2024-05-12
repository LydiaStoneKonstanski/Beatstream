
from pred_models import bq_client, upload_file_to_gcs, load_parquet_bq
import os
from enum import Enum


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

        self.df.to_parquet(parquet_path, engine='fastparquet')
        upload_file_to_gcs(parquet_path, bucket_name, gcs_blob_name)
        load_parquet_bq(gcs_uri, table_id, write_disposition.name)

        print(f"Load to cloud complete: {self.table_name}")

if __name__ == "__main__":

    table_name = 'model_a'
    table_description = """
    """
    query = """ SELECT userId, ts, song_id  FROM dbt_pk.event LIMIT 10"""
    df = bq_client.query(query).to_dataframe()

    model_a = PredictiveModel(table_name, table_description, df)
    model_a.upload_df_to_cloud(Disposition.TRUNCATE)
