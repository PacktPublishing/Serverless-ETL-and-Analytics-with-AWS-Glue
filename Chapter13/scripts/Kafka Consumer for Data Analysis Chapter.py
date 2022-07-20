import sys
import datetime
import boto3
import base64
from pyspark.sql import DataFrame, Row
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue import DynamicFrame
from datetime import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME','TARGET_BUCKET'])



spark = SparkSession.builder.config('spark.serializer','org.apache.spark.serializer.KryoSerializer').config('spark.sql.hive.convertMetastoreParquet','false').getOrCreate()
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

checkpoint_location = 's3://'+args['TARGET_BUCKET']+'/temp-dir/checkpoint-hudi-streaming/'

employees_schema = StructType([  StructField("emp_no", IntegerType()), StructField("name", StringType()), StructField("department", StringType()), StructField("city", StringType()), StructField("salary", IntegerType()), StructField("record_creation_time", DoubleType())])

def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        constructed_df = data_frame.select(from_json("value", schema=employees_schema).alias('data')).select('data.*')
        final_df = constructed_df.withColumn("record_creation_time", from_unixtime(constructed_df.record_creation_time,"yyyy-MM-dd HH:mm:ss:SSS"))
        # Write to S3 Sink
        commonConfig = {'className' : 'org.apache.hudi', 'hoodie.datasource.hive_sync.use_jdbc':'false', 'hoodie.datasource.write.precombine.field': 'record_creation_time', 'hoodie.datasource.write.recordkey.field': 'emp_no', 'hoodie.table.name': 'employees_cow_streaming', 'hoodie.consistency.check.enabled': 'true', 'hoodie.datasource.hive_sync.database': 'chapter-data-analysis-glue-database', 'hoodie.datasource.hive_sync.table': 'employees_cow_streaming', 'hoodie.datasource.hive_sync.enable': 'true', 'path': 's3://'+args['TARGET_BUCKET']+'/hudi/employees_cow_streaming'}
        unpartitionDataConfig = {'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor', 'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator'}
        incrementalConfig = {'hoodie.upsert.shuffle.parallelism': 3, 'hoodie.datasource.write.operation': 'upsert', 'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS', 'hoodie.cleaner.commits.retained': 10}
        combinedConf = {**commonConfig, **unpartitionDataConfig, **incrementalConfig}
        glueContext.write_dynamic_frame.from_options(frame = DynamicFrame.fromDF(final_df, glueContext, "final_df"), connection_type = "marketplace.spark", connection_options = combinedConf)

    
source_kafka_streaming_df = glueContext.create_data_frame.from_options(
    connection_type="kafka",
    connection_options={
        "connectionName": "chapter-data-analysis-msk-connection",
        "startingOffsets": "earliest",
        "topicName": "chapter-data-analysis",
        "classification": "text",
        "typeOfData": "kafka",
    },
    transformation_ctx="read_kafka_source",
)
# #"inferSchema": "true",
glueContext.forEachBatch(frame = source_kafka_streaming_df, batch_function = processBatch, options = {"windowSize": "10 seconds", "checkpointLocation": checkpoint_location})

job.commit()