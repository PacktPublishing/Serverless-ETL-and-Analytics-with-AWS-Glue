import sys
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when, lit
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import *
from datetime import datetime
from delta.tables import *

args = getResolvedOptions(sys.argv, ['JOB_NAME','TARGET_BUCKET','DELTALAKE_CONNECTION_NAME'])

spark = SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
tableLocation = 's3://'+args['TARGET_BUCKET']+'/deltalake_employees/'
inputDyf = glueContext.create_dynamic_frame_from_catalog(database='chapter-data-analysis-glue-database', table_name='employees')

commonConfig = {"path": tableLocation,"connectionName":args['DELTALAKE_CONNECTION_NAME']}

glueContext.write_dynamic_frame.from_options(frame=inputDyf, connection_type="marketplace.spark", connection_options=commonConfig)

deltaTable = DeltaTable.forPath(spark, tableLocation)
deltaTable.generate("symlink_format_manifest")

spark.sql("CREATE TABLE `chapter-data-analysis-glue-database`.employees_deltalake (emp_no int, name string, department string, city string, salary int) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '"+tableLocation+"_symlink_format_manifest/'")

job.commit()