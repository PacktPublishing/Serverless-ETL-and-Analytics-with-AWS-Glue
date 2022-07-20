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

args = getResolvedOptions(sys.argv, ['JOB_NAME','TARGET_BUCKET'])

spark = SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
tableLocation = 's3://'+args['TARGET_BUCKET']+'/deltalake_employees/'
deltaTable = DeltaTable.forPath(spark, tableLocation)

deltaTable.update(
  condition = "emp_no = 3",
  set = { "salary": "70000" ,"city":"'Cincinnati'" }
)

deltaTable.generate("symlink_format_manifest")

job.commit()