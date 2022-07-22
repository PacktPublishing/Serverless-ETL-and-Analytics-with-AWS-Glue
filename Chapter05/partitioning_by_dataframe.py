from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import to_timestamp

# Set your source S3 bucket path in which you put the downloaded data.json, and 
#   your target S3 bucket and path where this Glue Spark job writes your data with partitioning
SRC_S3_PATH = '<Set the S3 location where you put data.json>'
DST_S3_PATH = '<Set the S3 location where your Glue Spark job write the data with compression>'


if __name__ == '__main__':
    # Initialize GlueContext and SparkSession objects
    glue_context = GlueContext(SparkContext())
    spark = glue_context.spark_session

    # Read the data in the source S3 bucket by Spark DataFrame with schema inference option
    df = spark.read.option('inferSchema', True).json(SRC_S3_PATH)

    # Convert `datetime` column type from String to timestamp, and the column name to `datetime_ts`
    df_with_timestamp = df.withColumn('datetime_ts', to_timestamp('datetime')).drop('datetime')

    # Write the parquet files to the destination S3 bucket by Spark DataFrame with `category` partitioning
    df_with_timestamp.write\
        .partitionBy('category')\
        .parquet(DST_S3_PATH)

    '''
    After the Glue job is completed, you can see the following S3 folder structure:
    DST_S3_PATH (e.g. s3://your-bucket/path/)
    |-- category=drink/part-*****.snappy.parquet
    |-- category=grocery/part-*****.snappy.parquet
    |-- category=kitchen/part-*****.snappy.parquet
    '''