import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','DOMAIN_ENDPOINT'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

inputDyf = glueContext.create_dynamic_frame_from_catalog(database='chapter-data-analysis-glue-database', table_name='employees')#.toDF()

glueContext.write_dynamic_frame.from_options( frame=inputDyf, connection_type="marketplace.spark",
    connection_options={
        "es.nodes": args['DOMAIN_ENDPOINT'],
        "connectionName": "opensearch-connection",
        "es.nodes.wan.only": "true",
        "es.port": "443",
        "es.net.ssl": "true",
        "path": "employees/_doc"
    },
    transformation_ctx="es_write")

job.commit()