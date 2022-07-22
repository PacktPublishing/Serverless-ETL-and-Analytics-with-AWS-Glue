from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import to_timestamp

# Set your source S3 bucket path in which you put the downloaded data.json, and 
#   your target S3 bucket and path where this Glue Spark job writes your data with compression
SRC_S3_PATH = '<Set the S3 location where you put data.json>'
DST_S3_PATH = '<Set the S3 location where your Glue Spark job write the data with compression>'
COMPRESSION_CODEC = 'zstd'  # You can also set 'gzip', 'bzip2', 'snappy' and so on.


if __name__ == '__main__':
    # Initialize GlueContext and SparkSession objects
    glue_context = GlueContext(SparkContext())
    spark = glue_context.spark_session

    # Read the data in the source S3 bucket by Spark DataFrame with schema inference option
    df = spark.read.option('inferSchema', True).json(SRC_S3_PATH)

    # Convert `datetime` column type from String to timestamp, and the column name to `datetime_ts`
    df_with_timestamp = df.withColumn('datetime_ts', to_timestamp('datetime')).drop('datetime')
    '''
    df schema vs. df_with_timestamp schema
    [df.printSchema()]
    root
    |-- category: string (nullable = true)
    |-- customer_id: long (nullable = true)
    |-- datetime: string (nullable = true) 
    |-- order_id: string (nullable = true)
    |-- price: string (nullable = true)
    |-- product_name: string (nullable = true)

    [df_with_timestamp.printSchema()]
    root
    |-- category: string (nullable = true)
    |-- customer_id: long (nullable = true)
    |-- order_id: string (nullable = true)
    |-- price: string (nullable = true)
    |-- product_name: string (nullable = true)
    |-- datetime_ts: timestamp (nullable = true)  <= HERE
    '''

    # Write the parquet files to the destination S3 bucket by Spark DataFrame with compression
    df_with_timestamp.write.option('compression', COMPRESSION_CODEC).parquet(DST_S3_PATH)