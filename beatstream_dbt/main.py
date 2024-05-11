import os
import io
from pathlib import Path
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from google.auth import impersonated_credentials, default


#Set the environment variable to the path of your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/deepa/Documents/Projects/Beatstream/beatstream_dbt/gcloud_key/dbt-demo-422300-f22c1de9a426.json"


# the code below up to if else is just to check if the env var is set up correctly
variable_value = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")



# Check if the environment variable exists and print its value
if variable_value:
    print(f"The value of ENV_VARIABLE_NAME is: {variable_value}")
else:
    print("The environment variable is not set.")


# creds, pid = default()  # takes the credentials from the key file specified by GOOGLE_APPLICATION_CREDENTIAL var

service_account_file = "/Users/deepa/Documents/Projects/Beatstream/beatstream_dbt/gcloud_key/dbt-demo-422300-f22c1de9a426.json"
# Initialize credentials using the service account key file
creds = service_account.Credentials.from_service_account_file(service_account_file)
print("Credentials object:", creds)


# Set up impersonated credentials for the service account
impersonated_account_email = "dbtservacct@dbt-demo-422300.iam.gserviceaccount.com"


myimpersonated_credentials = impersonated_credentials.Credentials(
    source_credentials=creds,
    target_principal=impersonated_account_email,
    target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

######### Now you can use `impersonated_credentials` for authentication with Google Cloud services ####################

# Initialize Google Cloud clients for storage and bigquery
storage_client = storage.Client(project="dbt-demo-422300", credentials=myimpersonated_credentials)
bq_client = bigquery.Client(project="dbt-demo-422300", credentials=myimpersonated_credentials)

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
def load_parquet_bq(source_gcs_uri, target_table_id, write_disposition='TRUNCATE', target_schema=None ):
    print("Write disposition : {}".format(write_disposition))
    bq_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
           )

    if target_schema :
        job_config.schema = target_schema

    
    if write_disposition == 'TRUNCATE':
             job_config.write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    elif write_disposition == 'APPEND':
             job_config.write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    elif write_disposition == 'EMPTY':
             job_config.write_disposition=bigquery.WriteDisposition.WRITE_EMPTY
    

    load_job = bq_client.load_table_from_uri(
        source_gcs_uri, target_table_id, job_config=job_config
    )  # Make a bigquery API request.

    job_result=load_job.result()  # Waits for the job to complete.

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


    ###### Code for sweeping mulitple files for one Entity/Table ######
    ###### Repeat as becessary or create a function ###################

    # Set the local directory containing Parquet files
    #parquet_dir = "C:/Users/prave/DBT/datain/"
    parquet_dir = "/Users/deepa/Documents/Projects/Beatstream/data/parquets"

    # Set the GCS bucket name
    bucket_name = 'dbtdemo'

    # Set the target BigQuery table ID
    table_id = "dbt-demo-422300.dbt_pk.part_snappy_stage"

    # Get all files with .parquet extension in the directory
    parquet_files = [file for file in os.listdir(parquet_dir) if file.endswith('.parquet')]

    # Initialize BigQuery client
    bq_client = bigquery.Client()

     
    	
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
        write_disposition = 'TRUNCATE' if parquet_files.index(parquet_file)  == 0 else 'APPEND'


        # Load Parquet data into BigQuery table
        load_parquet_bq(gcs_uri, table_id, write_disposition)

        # Rename the Parquet file to mark it as processed
        os.rename(parquet_file_path, os.path.join(parquet_dir, f"{parquet_file}.processed"))

    print("All Parquet files processed and loaded into BigQuery.")

    ########## End of code block for sweeping mulitple files for one Entity/Table ###################
