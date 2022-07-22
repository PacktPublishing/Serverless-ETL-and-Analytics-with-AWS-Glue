# Chapter 12
# Code Snippet 01 - MongoDB Aggregation Pipelines

import sys
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

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

# pipeline parameter takes the custom aggregation pipeline JSON.
# Customize parameters for your tests as necessary
pipelineJSON = """{"$match":{"type":"apple"}}"""
mongo_options = {
    "uri": "MONGO_CONN_STR",
    "database": "test",
    "pipeline": pipelineJSON,
    "collection": "inventory",
    "username": "mongodb_test",
    "password": "XXXXXXX",
    "partitioner": "MongoSamplePartitioner",
    "partitionerOptions.partitionSizeMB": "10",
    "partitionerOptions.partitionKey": "_id"
}

# Create DynamicFrame
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="mongodb",
    connection_options=mongo_options
)

job.commit()