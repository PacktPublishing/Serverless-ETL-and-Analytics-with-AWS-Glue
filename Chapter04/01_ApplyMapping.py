# Chapter 04
# Code Snippet 01 - AWS Glue Transformations - ApplyMapping

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

# Function definition: Sample data generation
def createSampleDynamicFrameForApplyMapping():
    data = glueContext.spark_session.createDataFrame(
            [
                ('foo@company1.com',(1, "foo")),
                ('bar@company2.com',(2, "bar")),
            ],
            StructType(
                [
                    StructField('email', StringType()),
                    StructField('employee', StructType([
                        StructField('employee_id', IntegerType()),
                        StructField('employee_name', StringType())])
                    )
                ]
            )
    )
    return DynamicFrame.fromDF(data, glueContext, "sample_nested_data")

datasource0 = createSampleDynamicFrameForApplyMapping()

# Schema:
# root
# |-- email: string
# |-- employee: struct
# |    |-- employee_id: int
# |    |-- employee_name: string
datasource0.printSchema()

# Mapping List
mappingList = [
        (
            "email", "string", 
            "employee.employee_email", "string"
        ), 
        (
            "employee.employee_id", "int", 
            "employee.employee_id", "int"
        ), 
        (
            "employee.employee_name", "string", 
            "employee.employee_name", "string"
        )
    ]

# Calling ApplyMapping Transform
applyMapping0 = ApplyMapping.apply(
    frame=datasource0, 
    mappings=mappingList
)

# Schema:
# root
# |-- employee: struct
# |    |-- employee_email: string
# |    |-- employee_id: int
# |    |-- employee_name: string
applyMapping0.printSchema()

sink = glueContext.write_dynamic_frame.from_options(
    frame = applyMapping0, 
    connection_type = "s3", 
    connection_options = {"path": "s3://TARGET/prefix/"}, 
    format = "[__OUTPUT_FORMAT__]"
)

job.commit()