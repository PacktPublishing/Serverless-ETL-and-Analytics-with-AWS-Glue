# Chapter 04
# Code Snippet 03 - AWS Glue Transformations - Unbox

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

def createSampleDynamicFrameForUnbox():
    jsonString = """{
      "jsonrecords": [
        {
          "company": "DummyCorp1",
          "employees": [
            {
              "email": "foo@company1.com",
              "name": "foo1"
            },
            {
              "email": "bar@company1.com",
              "name": "bar1"
            }
          ]
        },
        {
          "company": "DummyCorp2",
          "employees": [
            {
              "email": "foo@company2.com",
              "name": "foo2"
            },
            {
              "email": "bar@company2.com",
              "name": "bar2"
            }
          ]
        },
        {
          "company": "DummyCorp3",
          "employees": [
            {
              "email": "foo@company3.com",
              "name": "foo3"
            },
            {
              "email": "bar@company3.com",
              "name": "bar3"
            }
          ]
        }
      ]
    }"""
    company_directory = [['Seattle, WA',jsonString]]
    company_directory_schema = StructType([StructField("location", StringType()) ,StructField("companies_json", StringType())]) 
    data = spark.createDataFrame(company_directory, schema = company_directory_schema)
    return DynamicFrame.fromDF(data, glueContext, "sample_nested_data")

datasource3 = createSampleDynamicFrameForUnbox()

# Output: 
# root
# |-- location: string
# |-- companies_json: string
datasource3.printSchema()

# Output: 
# +-----------+--------------------+
# |   location|      companies_json|
# +-----------+--------------------+
# |Seattle, WA|{
#       "jsonreco...|
# +-----------+--------------------+
datasource3.toDF().show()

# Calling Unbox
unbox0 = Unbox.apply(
    frame = datasource3, 
    path = "companies_json", 
    format = "json"
)

# Output: 
# +-----------+--------------------+
# |   location|      companies_json|
# +-----------+--------------------+
# |Seattle, WA|{[{DummyCorp1, [{...|
# +-----------+--------------------+
unbox0.toDF().show()

# Output: 
# root
# |-- location: string
# |-- companies_json: struct
# |    |-- jsonrecords: array
# |    |    |-- element: struct
# |    |    |    |-- company: string
# |    |    |    |-- employees: array
# |    |    |    |    |-- element: struct
# |    |    |    |    |    |-- email: string
# |    |    |    |    |    |-- name: string
unbox0.printSchema()

job.commit()