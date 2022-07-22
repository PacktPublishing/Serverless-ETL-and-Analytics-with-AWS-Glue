# Chapter 03
# Code Snippet 09 - Ingest data from JDBC - AWS Glue DynamicFrames

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

# Sample data used - https://dev.mysql.com/doc/world-x-setup/en/
# you can install and setup a mysql database in any location accessible to you. 
# You can also set this up locally on your workstation if you are testing this code locally using Glue local development libraries (or docker image)
# Change the following parameters according to your setup

mysql_options = {
    "url": "jdbc:mysql://DB_HOST:3306/world_x",
    "dbtable": "city",
    # "secretId": "glue_sec/mysqltestdb" # ARN or Name of Secret in AWS Secrets Manager. Remove user and password parameter if secretId is being used
    "user": "admin",
    "password": "password"
}

dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options=mysql_options
)

sink = glueContext.write_dynamic_frame.from_options(
    frame = dyf, 
    connection_type = "s3", 
    connection_options = {"path": "s3://TARGET/prefix/"}, 
    format = "parquet"
)

job.commit()