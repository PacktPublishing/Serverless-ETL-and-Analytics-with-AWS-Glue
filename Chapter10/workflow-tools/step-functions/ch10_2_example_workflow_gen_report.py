import sys
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame


def set_s3_path(bucket_and_path: str) -> str:
    if bucket_and_path.endswith("/"):
        return bucket_and_path[:-1]
    else:
        return bucket_and_path

args = getResolvedOptions(sys.argv, ['JOB_NAME','datalake_location', 'database', 'table', 'report_year'])
DATALAKE_LOCATION = set_s3_path(bucket_and_path=args['datalake_location'])
DATABASE = args['database']
TABLE = args['table']
REPORT_YEAR = args['report_year']
GEN_REPORT_QUERY = f"""
SELECT
    category,
    CAST(sum(price) as long) as total_sales,
    year(to_timestamp(datetime)) as report_year
FROM {DATABASE}.{TABLE}
WHERE year(to_timestamp(datetime)) = {REPORT_YEAR}
GROUP BY category, report_year
ORDER BY category, report_year DESC
"""


if __name__ == '__main__':
    glue_context = GlueContext(SparkContext())
    spark = glue_context.spark_session
    
    df = spark.sql(GEN_REPORT_QUERY)\
                .repartition("report_year")
    df.show(n=df.count(), truncate=False)  # show the report result in the output

    # Create a report table
    sink = glue_context.getSink(
        connection_type="s3",
        path=f"{DATALAKE_LOCATION}/serverless-etl-and-analysis-w-glue/chapter10/example-workflow-sfn/report/",
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["report_year"])
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName=f"{TABLE}_report")
    sink.writeFrame(DynamicFrame.fromDF(dataframe=df, glue_ctx=glue_context, name='dyf'))
