import os
import io
from pathlib import Path
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from google.auth import impersonated_credentials, default

# Set the environment variable to the path of your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/deepa/Documents/Projects/Beatstream/beatstream_dbt/gcloud_key/dbt-demo-422300-f22c1de9a426.json"

# the code below up to if else is just to check if the env var is set up correctly
variable_value = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

# Check if the environment variable exists and print its value
if variable_value:
    print(f"The value of ENV_VARIABLE_NAME is: {variable_value}")
else:
    print("The environment variable is not set.")



credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
if os.path.exists(credentials_path):
    print(f"The credentials file exists at: {credentials_path}")
else:
    print(f"The credentials file does NOT exist at: {credentials_path}")

creds, pid = default()  # takes the credentials from the key file specified by GOOGLE_APPLICATION_CREDENTIAL var

# Set up impersonated credentials for the service account
impersonated_account_email = "dbtservacct@dbt-demo-422300.iam.gserviceaccount.com"

myimpersonated_credentials = impersonated_credentials.Credentials(
    source_credentials=creds,   #the credentials of the service account or user running the script.
    target_principal=impersonated_account_email,   #
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

    #retrieving and printing the number of rows loaded
    destination_table = bq_client.get_table(target_table_id)
    print("Loaded {} rows into table {}.".format(destination_table.num_rows, target_table_id))
    # Check if the job completed successfully
    if job_result.state == 'DONE':
        return True
    else:
        return False


########## NOT USED  Create an external table in BigQuery
def create_external_table(gcs_uri, external_table_id):
    external_table_schema = [
        bigquery.SchemaField("index", "INTEGER"),
        bigquery.SchemaField("duration", "FLOAT64"),
        bigquery.SchemaField("end_of_fade_in", "FLOAT64"),
        # Add other fields here
    ]

    external_table_options = bigquery.ExternalConfig("CSV")
    external_table_options.schema = external_table_schema
    external_table_options.skip_leading_rows = 1

    external_table_ref = bq_client.dataset("your_dataset").table(external_table_id)
    external_table = bigquery.Table(external_table_ref, external_data_configuration=external_table_options)
    bq_client.create_table(external_table, location="US")
    print(f"External table {external_table_id} created with GCS URI: {gcs_uri}")


########## NOT USED  Example code to get schema (optional)
def get_table_schema():
    # Get the table reference
    table_ref = bq_client.dataset('dbt_pk').table('part_snappy')

    # Fetch the table schema
    table = bq_client.get_table(table_ref)

    # Extract schema information
    schema_info = [f"{schema.name} {schema.field_type}" for schema in table.schema]

    # Print the schema
    print("\n".join(schema_info))

    return


##### Main block execution #####

if __name__ == "__main__":
    # Initialize BigQuery client
    bq_client = bigquery.Client()

    ###### Code for sweeping mulitple files for one Entity/Table ######
    ###### Repeat as becessary or create a function ###################
    ###### THis Block of code is for 1st Event      ###################

    # Set the local directory containing Parquet files
    parquet_dir = "/Users/deepa/Documents/Projects/Beatstream/data/events"


# Set the GCS bucket name
bucket_name = 'dbtdemo'

# Set the target BigQuery table ID
table_id = "dbt-demo-422300.dbt_pk.event_stage"

# Get all files with .parquet extension in the directory
parquet_files = [file for file in os.listdir(parquet_dir) if file.endswith('.parquet')]


# Iterate over each Parquet file. If the parquet files are exact same there is no need to define the schema explictly
# The BQ table will be created if it does not exist based on the schema in the first file and any subsequent files and batches for the same file strcuture will be loaded using that schema
for parquet_file in parquet_files:
    # Define the full path to the Parquet file
    parquet_file_path = os.path.join(parquet_dir, parquet_file)
    print("Processing {} ....".format(parquet_file_path))

    # Set the destination blob name in GCS
    gcs_blob_name = f"{parquet_file}"

    # Upload the Parquet file to GCS
    upload_file_to_gcs(parquet_file_path, bucket_name, gcs_blob_name)

    # Define the GCS URI for the Parquet file
    gcs_uri = f"gs://{bucket_name}/{gcs_blob_name}"

    ######## Load file from GCS to bibquery table ############
    # Determine write disposition based on whether it's the first file or not to force table truncate to wipe out data loaded in previous batch
    write_disposition = 'TRUNCATE' if parquet_files.index(parquet_file) == 0 else 'APPEND'

    # Load Parquet data into BigQuery table
    load_parquet_bq(gcs_uri, table_id, write_disposition)

    # Rename the Parquet file to mark it as processed
    os.rename(parquet_file_path, os.path.join(parquet_dir, f"{parquet_file}.processed"))

print("All Parquet files for 1st event processed and loaded into BigQuery.")

###### End of Block of code for 1st Event      ###################

###### THis Block of code is to load individual dimension files    ###################

### todo 1 load user Block of code for Loading user.parquet ######
# Set the local directory containing Parquet files
parquet_dir = "/Users/deepa/Documents/Projects/Beatstream/data/dimensions"

# Set the GCS bucket name
bucket_name = 'dbtdemo'

# Load USER Table

# Set the target BigQuery table ID
table_id = "dbt-demo-422300.dbt_pk.user_stage"

# Get all files with .parquet extension in the directory
parquet_file = "user.parquet"

parquet_file_path = os.path.join(parquet_dir, parquet_file)

# Check if the file exists
if os.path.exists(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]):
    print("File exists!")

    print("Processing {} ....".format(parquet_file_path))

    # Set the destination blob name in GCS
    gcs_blob_name = f"{parquet_file}"

    # Upload the Parquet file to GCS
    upload_file_to_gcs(parquet_file_path, bucket_name, gcs_blob_name)

    # Define the GCS URI for the Parquet file
    gcs_uri = f"gs://{bucket_name}/{gcs_blob_name}"

    ######## Load file from GCS to bibquery table ############
    # Determine write disposition based on whether it's the first file or not to force table truncate to wipe out data loaded in previous batch
    write_disposition = 'TRUNCATE'

    # Load Parquet data into BigQuery table
    load_parquet_bq(gcs_uri, table_id, write_disposition)

    # Rename the Parquet file to mark it as processed
    os.rename(parquet_file_path, os.path.join(parquet_dir, f"{parquet_file}.processed"))

    print("User.parquet processed and loaded into BigQuery.")
else:
    print("File does not exist.")

    ###### todo 1 user end End of Block of code to load user      ###################

    ### todo 2(artist) Block of code for Loading user.parquet ######
    # Set the local directory containing Parquet files
    parquet_dir = "/Users/deepa/Documents/Projects/Beatstream/data/dimensions"

    # Set the GCS bucket name
    bucket_name = 'dbtdemo'

    # Load USER Table

    # Set the target BigQuery table ID
    table_id = "dbt-demo-422300.dbt_pk.artist_stage"

    # Get all files with .parquet extension in the directory
    parquet_file = "artist.parquet"

    parquet_file_path = os.path.join(parquet_dir, parquet_file)

    # Check if the file exists
    if os.path.exists(parquet_file_path):
        print("File exists!")

        print("Processing {} ....".format(parquet_file_path))

        # Set the destination blob name in GCS
        gcs_blob_name = f"{parquet_file}"

        # Upload the Parquet file to GCS
        upload_file_to_gcs(parquet_file_path, bucket_name, gcs_blob_name)

        # Define the GCS URI for the Parquet file
        gcs_uri = f"gs://{bucket_name}/{gcs_blob_name}"

        ######## Load file from GCS to bibquery table ############
        # Determine write disposition based on whether it's the first file or not to force table truncate to wipe out data loaded in previous batch
        write_disposition = 'TRUNCATE'

        # Load Parquet data into BigQuery table
        load_parquet_bq(gcs_uri, table_id, write_disposition)

        # Rename the Parquet file to mark it as processed
        os.rename(parquet_file_path, os.path.join(parquet_dir, f"{parquet_file}.processed"))

        print("User.parquet processed and loaded into BigQuery.")
    else:
        print("File does not exist.")

    ###### todo 2 artist end End of Block of code to load user      ###################

    ### todo 3(song) Block of code for Loading user.parquet ######
    # Set the local directory containing Parquet files
    parquet_dir = "/Users/deepa/Documents/Projects/Beatstream/data/dimensions"

    # Set the GCS bucket name
    bucket_name = 'dbtdemo'

    # Load USER Table

    # Set the target BigQuery table ID
    table_id = "dbt-demo-422300.dbt_pk.song_stage"

    # Get all files with .parquet extension in the directory
    parquet_file = "song.parquet"

    parquet_file_path = os.path.join(parquet_dir, parquet_file)

    # Check if the file exists
    if os.path.exists(parquet_file_path):
        print("File exists!")

        print("Processing {} ....".format(parquet_file_path))

        # Set the destination blob name in GCS
        gcs_blob_name = f"{parquet_file}"

        # Upload the Parquet file to GCS
        upload_file_to_gcs(parquet_file_path, bucket_name, gcs_blob_name)

        # Define the GCS URI for the Parquet file
        gcs_uri = f"gs://{bucket_name}/{gcs_blob_name}"

        ######## Load file from GCS to bibquery table ############
        # Determine write disposition based on whether it's the first file or not to force table truncate to wipe out data loaded in previous batch
        write_disposition = 'TRUNCATE'

        # Load Parquet data into BigQuery table
        load_parquet_bq(gcs_uri, table_id, write_disposition)

        # Rename the Parquet file to mark it as processed
        os.rename(parquet_file_path, os.path.join(parquet_dir, f"{parquet_file}.processed"))

        print("User.parquet processed and loaded into BigQuery.")
    else:
        print("File does not exist.")

    ###### todo 3 song end End of Block of code to load user      ###################

     ### todo 4 (event) Block of code for Loading user.parquet ######
     # Set the local directory containing Parquet files
    parquet_dir = "/Users/deepa/Documents/Projects/Beatstream/data/events"

            # Set the GCS bucket name
    bucket_name = 'dbtdemo'

    # Load USER Table

    # Set the target BigQuery table ID
    table_id = "dbt-demo-422300.dbt_pk.event_stage"

    # Get all files with .parquet extension in the directory
    parquet_file = "event.parquet"

    parquet_file_path = os.path.join(parquet_dir, parquet_file)

    # Check if the file exists
    if os.path.exists(parquet_file_path):
        print("File exists!")

        print("Processing {} ....".format(parquet_file_path))

        # Set the destination blob name in GCS
        gcs_blob_name = f"{parquet_file}"

        # Upload the Parquet file to GCS
        upload_file_to_gcs(parquet_file_path, bucket_name, gcs_blob_name)

        # Define the GCS URI for the Parquet file
        gcs_uri = f"gs://{bucket_name}/{gcs_blob_name}"

        ######## Load file from GCS to bibquery table ############
        # Determine write disposition based on whether it's the first file or not to force table truncate to wipe out data loaded in previous batch
        write_disposition = 'TRUNCATE'

        # Load Parquet data into BigQuery table
        load_parquet_bq(gcs_uri, table_id, write_disposition)

        # Rename the Parquet file to mark it as processed
        os.rename(parquet_file_path, os.path.join(parquet_dir, f"{parquet_file}.processed"))

        print("User.parquet processed and loaded into BigQuery.")
    else:
        print("File does not exist.")


########## End of code block for sweeping mulitple files for one Entity/Table ###################
