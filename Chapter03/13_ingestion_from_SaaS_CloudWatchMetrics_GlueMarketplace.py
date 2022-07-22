# Chapter 03
# Code Snippet 13 - Ingest data from SaaS - CloudWatchMetrics ingestion using Marketplace connector

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

# Prerequisites: 
# 1. subscribe to Glue marketplace connector for CloudWatch Metrics and create a connection named "CloudWatchMetricsConnector"
# 2. Target s3 location, catalog database and table

dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="marketplace.athena",
    connection_options={
        "schemaName": "default",
        "tableName": "metrics",
        "connectionName": "CloudWatchMetricsConnector",
    }
)

target = glueContext.getSink(
    path="s3://bucket/target/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True
)
target.setCatalogInfo(
    catalogDatabase="default", 
    catalogTableName="cw_metrics"
)
target.setFormat("glueparquet")
target.writeFrame(dyf)

job.commit()