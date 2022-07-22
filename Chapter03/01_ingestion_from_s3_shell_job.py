# Chapter 03
# Code Snippet 01 - Ingest data from S3 - Python Shell Job.

import boto3
import io
import pandas as pd

client = boto3.client('s3')

# nyc-tlc - https://registry.opendata.aws/nyc-tlc-trip-records-pds/
# If you are not able to access s3 location s3://nyc-tlc for any reason, you can find a sample file with 1000 rows in "./sample_data/yellow_tripdata_2021-07.csv". 
# you can upload this to your own S3 location and use it instead of public nyc-tlc bucket location.

src_bucket = 'nyc-tlc' # SOURCE_S3_BUCKET_NAME
target_bucket = 'TARGET_S3_BUCKET_NAME'
src_object = client.get_object(
    Bucket=src_bucket, 
    Key='trip data/yellow_tripdata_2021-07.csv'
)

# Read CSV and Transform to JSON
df = pd.read_csv(src_object['Body'])
jsonBuffer = io.StringIO()
df.to_json(jsonBuffer, orient='records')

# Write JSON to target location
client.put_object(
    Bucket=target_bucket, 
    Key='target_prefix/data.json', 
    Body=jsonBuffer.getvalue()
)