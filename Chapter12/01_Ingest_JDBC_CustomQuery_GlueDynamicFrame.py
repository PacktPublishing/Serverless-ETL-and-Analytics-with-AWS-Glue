# Chapter 12
# Code Snippet 01 - Custom JDBC Query

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

# query parameter takes the custom query.
# postgresql is being used in the following example. However, this parameter will work similarly with other DB engines
# Customize parameters for your tests as necessary
connection_postgres_options = {
    "url": "jdbc:postgresql://HOSTNAME:5432/gluetest",
    "query": "select * FROM test where id < 100",
    "dbtable": "test",
    "secretId":"glue/postgres_test_db",
    "ssl": "true",
    "sslmode": "verify-full",
    "customJdbcDriverS3Path": "s3://S3_BUCKET/ postgresql.jar",
    "customJdbcDriverClassName": "org.postgresql.Driver"
    }

# Create DynamicFrame
datasource0 = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options=connection_postgres_options
)

job.commit()