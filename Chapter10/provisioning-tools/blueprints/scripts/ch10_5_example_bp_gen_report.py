import sys
import boto3
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame


def set_s3_path(bucket_and_path: str) -> str:
    if bucket_and_path.endswith("/"):
        return bucket_and_path[:-1]
    else:
        return bucket_and_path

def get_workflow_props(client, prop_name: str, args: dict) -> str:
    run_props = client.get_workflow_run_properties(
            Name=args['WORKFLOW_NAME'],
            RunId=args['WORKFLOW_RUN_ID'])['RunProperties']
    return run_props[prop_name]

glue = boto3.client("glue")
args = getResolvedOptions(sys.argv, ['JOB_NAME','WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])

DATALAKE_LOCATION = set_s3_path(bucket_and_path=get_workflow_props(client=glue, prop_name="datalake_location", args=args))
DATABASE = get_workflow_props(client=glue, prop_name="database", args=args)
TABLE = get_workflow_props(client=glue, prop_name="table", args=args)
TABLE_ANALYSIS = TABLE + '_analysis'
TABLE_REPORT = TABLE + '_report'
REPORT_YEAR = get_workflow_props(client=glue, prop_name="report_year", args=args)
GEN_REPORT_QUERY = f"""
SELECT
    category,
    CAST(sum(price) as long) as total_sales,
    report_year
FROM {DATABASE}.{TABLE_ANALYSIS}
WHERE report_year = {REPORT_YEAR}
GROUP BY category, report_year
ORDER BY category, report_year DESC
"""


if __name__ == '__main__':
    glue_context = GlueContext(SparkContext())
    spark = glue_context.spark_session
    
    df = spark.sql(GEN_REPORT_QUERY)
    df.show(n=df.count(), truncate=False)  # show the report result in the output

    # Create/update a report table
    sink = glue_context.getSink(
        connection_type="s3",
        path=f"{DATALAKE_LOCATION}/serverless-etl-and-analysis-w-glue/chapter10/example-bp/report/",
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["report_year"])
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName=TABLE_REPORT)
    sink.writeFrame(DynamicFrame.fromDF(dataframe=df, glue_ctx=glue_context, name='dyf'))
