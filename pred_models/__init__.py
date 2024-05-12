import os
import io
from pathlib import Path
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from google.auth import impersonated_credentials, default

# Set the environment variable to the path of your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/lydia/Projects/Beatstream/data/key.json"

# the code below up to if else is just to check if the env var is set up correctly
variable_value = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

# Check if the environment variable exists and print its value
if variable_value:
    print(f"The value of ENV_VARIABLE_NAME is: {variable_value}")
else:
    print("The environment variable is not set.")

creds, pid = default()  # takes the credentials from the key file specified by GOOGLE_APPLICATION_CREDENTIAL var

# Set up impersonated credentials for the service account
impersonated_account_email = "dbtservacct@dbt-demo-422300.iam.gserviceaccount.com"

myimpersonated_credentials = impersonated_credentials.Credentials(
    source_credentials=creds,
    target_principal=impersonated_account_email,
    target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

######### Now you can use `impersonated_credentials` for authentication with Google Cloud services ####################


# Initialize Google Cloud clients for storage and bigquery

storage_client = storage.Client(credentials=myimpersonated_credentials)
bq_client = bigquery.Client(credentials=myimpersonated_credentials)


# Function to Upload a file to Google Cloud Storage
def upload_file_to_gcs(local_file_path, bucket_name, destination_blob_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file_path)
    print(f"File {local_file_path} uploaded to gs://{bucket_name}/{destination_blob_name}")


## Function to load parquet file
# source_gcs_uri = parquet soruce file
# write_disposition =  TRUNCATE = trucate the table and load file, APPEND=load file and append to existing rows, EMPTY=load only if table is empty
# target_table_id = "your-project.your_dataset.your_table_name"
# target_schema = target Schema definition - optional
# bq_client is the globally defined Big_Query Client object using GC credentials
def load_parquet_bq(source_gcs_uri, target_table_id, write_disposition='TRUNCATE', target_schema=None):
    print("Write disposition : {}".format(write_disposition))
    bq_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
    )

    if target_schema:
        job_config.schema = target_schema

    if write_disposition == 'TRUNCATE':
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    elif write_disposition == 'APPEND':
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    elif write_disposition == 'EMPTY':
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

    load_job = bq_client.load_table_from_uri(
        source_gcs_uri, target_table_id, job_config=job_config
    )  # Make a bigquery API request.

    job_result = load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(target_table_id)
    print("Loaded {} rows into table {}.".format(destination_table.num_rows, target_table_id))
    # Check if the job completed successfully
    if job_result.state == 'DONE':
        return True
    else:
        return False
