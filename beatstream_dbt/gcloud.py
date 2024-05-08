import os
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from google.auth import impersonated_credentials, default
variable_value = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
# Check if the environment variable exists and print its value
if variable_value:
  print(f"The value of ENV_VARIABLE_NAME is: {variable_value}")
else:
  print("The environment variable is not set.")
creds, pid = default()
# Set up impersonated credentials
impersonated_account_email = "dbtservacct@dbt-demo-422300.iam.gserviceaccount.com"
myimpersonated_credentials = impersonated_credentials.Credentials(
  source_credentials=creds,
  target_principal=impersonated_account_email,
  target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
# Initialize Google Cloud clients
storage_client = storage.Client(credentials=myimpersonated_credentials)
bq_client = bigquery.Client(credentials=myimpersonated_credentials)
# Upload a file to Google Cloud Storage

def upload_file_to_gcs(local_file_path, bucket_name, destination_blob_name):
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(destination_blob_name)
  blob.upload_from_filename(local_file_path)
  print(f"File {local_file_path} uploaded to gs://{bucket_name}/{destination_blob_name}")
if __name__ == "__main__":
  # Set the path to the local CSV file
  local_parquet_file_path = "/data/part-00002-29e86e04-74d9-46f1-9465-715832f4f993-c000.snappy.parquet"
  # Set the GCS bucket and blob names
  bucket_name = 'dbtdemo'
  destination_blob_name = 'sample.csv'
  # Upload the file to GCS
  upload_file_to_gcs(local_parquet_file_path, bucket_name, destination_blob_name)
