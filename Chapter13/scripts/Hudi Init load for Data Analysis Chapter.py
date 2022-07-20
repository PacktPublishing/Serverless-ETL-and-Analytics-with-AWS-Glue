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

args = getResolvedOptions(sys.argv, ['JOB_NAME','TARGET_BUCKET'])

spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer').config('spark.sql.hive.convertMetastoreParquet', 'false').getOrCreate()
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

inputDf = glueContext.create_dynamic_frame_from_catalog(database='chapter-data-analysis-glue-database', table_name='employees').toDF().withColumn('ts', lit(datetime.now()))

commonConfig = {'className': 'org.apache.hudi',
                'hoodie.datasource.hive_sync.use_jdbc': 'false',
                'hoodie.datasource.write.recordkey.field': 'emp_no',
                'hoodie.table.name': 'employees_cow',
                'hoodie.consistency.check.enabled': 'true',
                'hoodie.datasource.hive_sync.database': 'chapter-data-analysis-glue-database',
                'hoodie.datasource.hive_sync.table': 'employees_cow',
                'hoodie.datasource.hive_sync.enable': 'true',
                'path': 's3://'+args['TARGET_BUCKET']+'/hudi/employees_cow_data'}

unpartitionDataConfig = {'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor',
                         'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator'}

initLoadConfig = {'hoodie.bulkinsert.shuffle.parallelism': 3,
                  'hoodie.datasource.write.operation': 'bulk_insert'}

combinedConf = {**commonConfig, **unpartitionDataConfig, **initLoadConfig}
glueContext.write_dynamic_frame.from_options(frame=DynamicFrame.fromDF(inputDf, glueContext, "inputDf"), connection_type="marketplace.spark", connection_options=combinedConf)

