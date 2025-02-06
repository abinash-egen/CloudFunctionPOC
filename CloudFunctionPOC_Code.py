import os
import json
import csv
from google.cloud import storage
from google.cloud import bigquery

# Define target bucket name
DEST_BUCKET = 'target_json_bucket'    # Target bucket where source files are converted to Json and loaded

# Schema definition for json file and bigquery table
schema_json = [
    bigquery.SchemaField("Item", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Store", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("State", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Qty", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Price", "NUMERIC",mode="NULLABLE"),
    bigquery.SchemaField("Total", "NUMERIC",mode="NULLABLE"),
]


#Function to convert CSV file to Json file
def convert_csv_to_json(csv_file):
    print(f"***********CSV TO JSON FILE************.   {csv_file}. ************************")
    data = []
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        data.append(json.dumps(row))
    return data

#Function to load Json data in GCS
def upload_json_format_to_gcs(json_data, destination_blob_name):
    client = storage.Client()
    bucket = client.get_bucket(DEST_BUCKET)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string("\n".join(json_data), content_type='application/json')
    print(f"***********Created JSON FILE************.   {destination_blob_name}. *******************")


def create_biglake_table_from_gcs(
    project_id: str,
    dataset_id: str,
    table_id: str,
    gcs_uri: str,  
    source_format: str,
    schema: list 
):
    try:
        client = bigquery.Client(project=project_id)
        dataset = client.get_dataset(dataset_id) 

        table_ref = dataset.table(table_id)  

        external_config = bigquery.ExternalConfig(source_format)
        external_config.source_uris = [gcs_uri]
        external_config.schema = schema

        table = bigquery.Table(table_ref, schema=schema)  
        table.external_data_configuration = external_config

        table = client.create_table(table) 

        print(f"*************Created BigLake table {table.full_table_id}. ****************")
        return table

    except Exception as e:
        print(f"***********Error creating BigLake table: {e}")
        return None


#Main cloud function entry point function
def process_file(data, context):
    file = data
    file_name = file['name']
    bucket_name = file['bucket']

    # Check if the file has a .csv extension
    if not file_name.lower().endswith('.csv'):
        print(f"****************Trigger file {file_name} is not a CSV file. Ignored.****************")
        return
    
    # Extract file extension and set target file name by replacing csv with json
    target_file_name = f"{os.path.splitext(file_name)[0]}.json"  

    # Download the CSV file from the source bucket to memory
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    file_content = blob.download_as_text()  # Read file content into memory
    print(f"*************File downloaded: {file_name}")

    # Convert CSV content to JSON
    json_data = convert_csv_to_json(file_content.splitlines())

    # Upload the JSON data to the destination bucket with the same filename
    upload_json_format_to_gcs(json_data, target_file_name)
    print(f"*****************File uploaded to target bucket: {target_file_name}")

    # Load the JSON file into BigLake
    print("*************Calling function to load TO BigQuery*******************")
    project_id = "qwiklabs-gcp-02-b845956d8b04"
    dataset_id = "BigLakeDS"
    table_id = "LakeTbl"
    source_format = "NEWLINE_DELIMITED_JSON" # For JSON arrays in separate lines

    #Need to pull all the Json file from the target bucket so, use *.json
    file_uri=f"gs://{DEST_BUCKET}/*.json"

    create_biglake_table_from_gcs(project_id, dataset_id, table_id, file_uri, source_format, schema=schema_json)