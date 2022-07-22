# Chapter 04
# Code Snippet 02 - AWS Glue Transformations - Relationalize, Join, RenameField

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
def createSampleDynamicFrameForRelationalize():
    jsonString = """
        [
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
        """
    data = glueContext.spark_session.read.json(sc.parallelize([jsonString]))
    return DynamicFrame.fromDF(data, glueContext, "sample_nested_data")

datasource1 = createSampleDynamicFrameForRelationalize()



# Schema:
# root
# |-- company: string
# |-- employees: array
# |    |-- element: struct
# |    |    |-- email: string
# |    |    |-- name: string
datasource1.printSchema()

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

# Calling Relationalize Transform
relationalize0 = Relationalize.apply(
    frame=datasource1, 
    staging_path='/tmp/glue_relationalize', 
    name='company'
)

# INFO GlueLogger: dict_keys(['company', 'company_employees'])
logger.info(str(relationalize0.keys()))

# selecting keys from DynamicFrameCollection
company_Frame = relationalize0.select('company')
emp_Frame = relationalize0.select('company_employees')

# Output:
# +----------+---------+
# |   company|employees|
# +----------+---------+
# |DummyCorp1|        1|
# |DummyCorp2|        2|
# |DummyCorp3|        3|
# +----------+---------+
company_Frame.toDF().show()

# Output:
# +---+-----+-------------------+------------------+
# | id|index|employees.val.email|employees.val.name|
# +---+-----+-------------------+------------------+
# |  1|    0|   foo@company1.com|              foo1|
# |  1|    1|   bar@company1.com|              bar1|
# |  2|    0|   foo@company2.com|              foo2|
# |  2|    1|   bar@company2.com|              bar2|
# |  3|    0|   foo@company3.com|              foo3|
# |  3|    1|   bar@company3.com|              bar3|
# +---+-----+-------------------+------------------+
emp_Frame.toDF().show()

# Calling Join
join0 = Join.apply(
    frame1 = company_Frame, 
    frame2 = emp_Frame, 
    keys1 = 'employees', 
    keys2 = 'id'
)

# Output: 
# +------------------+----------+---------+-----+-------------------+---+
# |employees.val.name|company   |employees|index|employees.val.email|id |
# +------------------+----------+---------+-----+-------------------+---+
# |foo2              |DummyCorp2|2        |0    |foo@company2.com   |2  |
# |bar2              |DummyCorp2|2        |1    |bar@company2.com   |2  |
# |foo3              |DummyCorp3|3        |0    |foo@company3.com   |3  |
# |bar3              |DummyCorp3|3        |1    |bar@company3.com   |3  |
# |foo1              |DummyCorp1|1        |0    |foo@company1.com   |1  |
# |bar1              |DummyCorp1|1        |1    |bar@company1.com   |1  |
# +------------------+----------+---------+-----+-------------------+---+
join0.toDF().show(truncate=False)

# Renaming Fields
renameField0 = RenameField.apply(
    frame = join0, 
    old_name = "`employees.val.email`", 
    new_name = "name"
)
renameField1 = RenameField.apply(
    frame = renameField0, 
    old_name = "`employees.val.name`", 
    new_name = "email"
)

# Output: 
# root
# |-- company: string
# |-- employees: long
# |-- name: string
# |-- index: int
# |-- email: string
# |-- id: long
renameField1.printSchema()

job.commit()