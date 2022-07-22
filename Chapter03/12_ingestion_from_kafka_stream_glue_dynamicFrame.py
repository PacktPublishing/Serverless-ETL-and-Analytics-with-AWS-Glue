# Chapter 03
# Code Snippet 12 - Ingest data from kafka stream

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

# Read from Kafka stream
data_frame_datasource0 = glueContext.create_data_frame.from_catalog(
    database = "default", 
    table_name = "kafka_stream", 
    transformation_ctx = "datasource0", 
    additional_options = {
        "startingOffsets": "earliest", 
        "inferSchema": "true"
    }
)

# ProcessBatch function to process microbatches and write to target
def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        datasource0 = DynamicFrame.fromDF(
            data_frame, 
            glueContext, 
            "from_data_frame"
        )
        now = datetime.datetime.now()
        path_datasink1 = "s3://bucket/destination/" + "/ingest_year=" + "{:0>4}".format(str(now.year)) + "/ingest_month=" + "{:0>2}".format(str(now.month)) + "/ingest_day=" + "{:0>2}".format(str(now.day)) + "/ingest_hour=" + "{:0>2}".format(str(now.hour)) + "/"
        datasink1 = glueContext.write_dynamic_frame.from_options(
            frame = datasource0, 
            connection_type = "s3", 
            connection_options = {
                "path": path_datasink1
            }, 
            format = "parquet", 
            transformation_ctx = "datasink1"
        )

glueContext.forEachBatch(
    frame = data_frame_datasource0, 
    batch_function = processBatch, 
    options = {
        "windowSize": "100 seconds", 
        "checkpointLocation": "s3://bucket/checkpoint_loc/"
    }
)

job.commit()