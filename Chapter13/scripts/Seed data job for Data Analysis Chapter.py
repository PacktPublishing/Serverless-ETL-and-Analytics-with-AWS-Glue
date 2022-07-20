import sys
from pyspark.sql.types import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','TARGET_BUCKET'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
             
job.init(args['JOB_NAME'], args)

employees_schema = StructType([  
                      StructField("emp_no", IntegerType()),
                      StructField("name", StringType()),
                      StructField("department", StringType()),
                      StructField("city", StringType()),
                      StructField("salary", IntegerType())
                    ])

employees_data = [
               [1, 'Adam', 'IT', 'SFO', 50000],\
               [2, 'Susan', 'Sales', 'NY', 60000],\
               [3, 'Jeff', 'Finance', 'Tokyo', 55000],\
               [4, 'Bill', 'Manufacturing', 'New Delhi', 70000],\
               [5, 'Joe', 'IT', 'Chicago', 45000],\
               [6, 'Steve', 'Finance', 'NY', 60000],\
               [7, 'Mike', 'IT', 'SFO', 60000]\
             ]

employees_df = spark.createDataFrame(employees_data, schema = employees_schema)

employees_dyf = DynamicFrame.fromDF(employees_df, glueContext, "employees_dyf")

sink = glueContext.getSink(connection_type="s3", path="s3://"+args['TARGET_BUCKET']+"/seed_data/employees",
    enableUpdateCatalog=True, updateBehavior="UPDATE_IN_DATABASE")
sink.setFormat("glueparquet")
sink.setCatalogInfo(catalogDatabase='chapter-data-analysis-glue-database', catalogTableName='employees')
sink.writeFrame(employees_dyf)

job.commit()