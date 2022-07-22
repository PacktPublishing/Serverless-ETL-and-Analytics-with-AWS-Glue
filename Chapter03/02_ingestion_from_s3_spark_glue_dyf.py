# Chapter 03
# Code Snippet 02 - Ingest data from S3 - AWS Glue Spark ETL using DynamicFrames

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize SparkContext, GlueContext and SparkSession.
params = []
if '--JOB_NAME' in sys.argv:
    params.append('JOB_NAME')
args = getResolvedOptions(sys.argv, params)
if 'JOB_NAME' in args:
    jobname = args['JOB_NAME']
else:
    jobname = "test"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

job = Job(glueContext)
job.init(jobname, args)

# Read CSV from S3 data store. 
# nyc-tlc - https://registry.opendata.aws/nyc-tlc-trip-records-pds/
# If you are not able to access s3 location s3://nyc-tlc for any reason, you can find a sample file with 1000 rows in "./sample_data/yellow_tripdata_2021-07.csv". 
# you can upload this to your own S3 location and use it instead of public nyc-tlc bucket location.

dy_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3", 
    connection_options = {"paths": ["s3://nyc-tlc/trip data/yellow_tripdata_2021-07.csv"]}, 
    format="csv", 
    format_options = {"withHeader": True}
)

# Writing output to target S3 bucket. Please update the Target S3 bucket name.
datasink = glueContext.write_dynamic_frame.from_options(
    frame = dy_frame, connection_type = "s3", 
    connection_options = {
    "path": "s3://TARGET_BUCKET_NAME/target_prefix/"
    }, 
    format = "json"
)

job.commit()