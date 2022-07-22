# Chapter 03
# Code Snippet 06 - Ingest data from S3 - Glue ETL Bounded Execution (by num files)

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

# DynamicFrame read from a catalog table backed by s3 location with a large number of objects with boundedFiles option to read 1000 objects at a time.
dyf_4000 = glueContext.create_dynamic_frame.from_catalog(
    database = "db_name",
    tableName = "four_thousand_file_table",
    transformation_ctx = "dyf_4000",
    additional_options = {"boundedFiles": "1000"}
)

# Writing output to target S3 bucket. Please update the Target S3 bucket name.
datasink = glueContext.write_dynamic_frame.from_options(frame = dyf_4000, connection_type = "s3", connection_options = {"path": "s3://TARGET_BUCKET_NAME/target_prefix/"}, format = "parquet")

job.commit()