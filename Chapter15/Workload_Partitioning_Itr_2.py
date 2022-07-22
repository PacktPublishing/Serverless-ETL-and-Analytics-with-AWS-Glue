# Sample Script demonstrating Simple Join operation with workload partitoning technique.

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Replace the database,table_name and column names for both source and target below per your use case in this script. 

#workload partitoning technique Implemented while reading the data from Glue Data Catalog 

tripsfactDyf = glueContext.create_dynamic_frame.from_catalog(database = "trips_fact_data", table_name = "trips_fact_data", transformation_ctx = "datasource0",additional_options = {"boundedFiles" : "95000"})

paymentDimDyf = glueContext.create_dynamic_frame.from_catalog(database = "workload_partitioning", table_name = "payments_dim_data", transformation_ctx = "datasource0", additional_options = {"boundedFiles" : "95000"})

applymapping1 = ApplyMapping.apply(frame = tripsfactDyf, mappings = [("trip_id", "long", "trip_id", "long"), ("vendor_name", "string", "vendor_name", "string"), ("trip_pickup_datetime", "string", "trip_pickup_datetime", "string"), ("trip_dropoff_datetime", "string", "trip_dropoff_datetime", "string"), ("passenger_count", "string", "passenger_count", "string"), ("trip_distance", "string", "trip_distance", "string"), ("payment_type", "string", "payment_type", "string"), ("fare_amt", "string", "fare_amt", "string"), ("surcharge", "string", "surcharge", "string"), ("total_amt", "string", "total_amt", "string"), ("tip_amt", "string", "tip_amt", "string"), ("tolls_amt", "string", "tolls_amt", "string"), ("mta_tax", "string", "mta_tax", "string"), ("year", "string", "year", "string"), ("month", "string", "month", "string")], transformation_ctx = "applymapping1")

applymapping2 = ApplyMapping.apply(frame = paymentDimDyf, mappings = [("trip_id", "long", "tripid", "long"), ("vendor_name", "string", "vendorname", "string"), ("payment_type", "string", "pymt_type", "string"), ("fare_amt", "string", "fareamt", "string"), ("surcharge", "string", "surchrg", "string"), ("total_amt", "string", "ttl_amt", "string"), ("tip_amt", "string", "tipamt", "string"), ("tolls_amt", "string", "tollsamt", "string"), ("mta_tax", "string", "mtatax", "string"), ("year", "string", "yearpmt", "string"), ("month", "string", "monthpmt", "string")], transformation_ctx = "applymapping2")

JoinFctDim = Join.apply(tripsfactDyf1, paymentDimDyf1, 'trip_id', 'tripid').drop_fields(['tripid', 'vendorname','pymt_type','fareamt','pymt_type','surchrg','ttl_amt','tipamt','tollsamt','mtatax','yearpmt','monthpmt'])

datasink2 = glueContext.write_dynamic_frame.from_options(frame = JoinFctDim, connection_type = "s3", connection_options = {"path": "s3://your-resultant-s3-bucket/taxiCuratedData"}, format = "parquet", transformation_ctx = "datasink2")

job.commit()
