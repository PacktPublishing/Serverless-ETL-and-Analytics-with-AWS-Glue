import sys
from pyspark.sql.types import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
import boto3

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','TARGET_BUCKET'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
             
job.init(args['JOB_NAME'], args)

#######################Start transaction and load data###########################
transactionId = glueContext.start_transaction(read_only=False)
employees_dyf = glueContext.create_dynamic_frame_from_catalog(database='chapter-data-analysis-glue-database', table_name='employees')

sink = glueContext.getSink(connection_type="s3", path="s3://"+args['TARGET_BUCKET']+"/employees_governed_table/", enableUpdateCatalog=True, updateBehavior="UPDATE_IN_DATABASE", transactionId=transactionId)
sink.setFormat("glueparquet")
sink.setCatalogInfo(catalogDatabase='chapter-data-analysis-glue-database', catalogTableName='employees_governed_table')
sink.writeFrame(employees_dyf)
glueContext.commit_transaction(transactionId)
#######################Start transaction and load data ends###########################
job.commit()