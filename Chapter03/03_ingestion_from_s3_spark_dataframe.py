# Chapter 03
# Code Snippet 03 - Ingest data from S3 - Spark Dataframes

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initializing SparkContext, GlueContext and SparkSession.
# Even if we are not using glueContext DynamicFrame APIs, 
# it is important to initialize GlueContext as this serves as the entrypoint 
# for many Glue features like metrics, continuous logging etc. when running this on AWS Glue.

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

df = spark.read.option("header","true").csv("s3://nyc-tlc/trip data/yellow_tripdata_2021-07.csv")

# Writing output to target S3 bucket. Please update the Target S3 bucket name.
df.write.json("s3://TARGET_BUCKET_NAME/target_prefix/")

job.commit()