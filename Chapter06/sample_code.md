# Chapter 06 sample code

## Technical requirements
Note: If you run the sample code on Glue Studio notebook or Glue Interactive Sessions, we recommend to execute following cell first.

```
%session_id_prefix chapter06-
%glue_version 3.0
%idle_timeout 60
```

## Normalizing data

### Casting data types and map column names

- Run the following code on AWS Glue
```
from pyspark.sql import Row
product = [
    {'product_id': '00001', 'product_name': 'Heater', 'product_price': '250'},
    {'product_id': '00002', 'product_name': 'Thermostat', 'product_price': '400'}
]

df_products = spark.createDataFrame(Row(**x) for x in product)
df_products.printSchema()
df_products.show()
```

- Run the following code on AWS Glue
```
from pyspark.sql.functions import col
df_mapped_dataframe = df_products \
    .withColumn("product_price", col("product_price").cast('integer')) \
    .withColumnRenamed("product_price", "price")

df_mapped_dataframe.printSchema()
df_mapped_dataframe.show()
```

- Run the following code on AWS Glue
```
df_products.createOrReplaceTempView("products")
df_mapped_sql = spark.sql("SELECT product_id, product_name, INT(product_price) as price from products")
df_mapped_sql.printSchema()
df_mapped_sql.show()
```

- Run the following code on AWS Glue
```
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue import DynamicFrame

glueContext = GlueContext(SparkContext.getOrCreate())
dyf = DynamicFrame.fromDF(df_products, glueContext, "from_df")

dyf = dyf.apply_mapping(
    [
        ('product_id', 'string', 'product_id', 'string'),
        ('product_name', 'string', 'product_name', 'string'),
        ('product_price', 'string', 'price', 'integer')
    ]
)
df_mapped_dyf = dyf.toDF()
df_mapped_dyf.printSchema()
df_mapped_dyf.show()
```

### Inferring schema

- Run the following code on AWS Glue
```
df_infer_schema_false = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", False) \
    .load("s3://covid19-lake/static-datasets/csv/CountyPopulation/County_Population.csv")
df_infer_schema_false.printSchema()
```

- Run the following code on AWS Glue
```
df_infer_schema_true = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("s3://covid19-lake/static-datasets/csv/CountyPopulation/County_Population.csv")
df_infer_schema_true.printSchema()
```

### Computing schema on-the-fly

- Save the following file as `sample.json`
```
{"id":"aaa","key":12}
{"id":"bbb","key":34}
{"id":"ccc","key":56}
{"id":"ddd","key":78}
{"id":"eee","key":"90"}
```

- Run the following command using AWS CLI
```
$ aws s3 cp sample.json s3://path_to_sample_data/
```

```
$ aws glue create-database --database-input Name=choice
```

```
$ aws glue create-crawler --name choice --database choice --role GlueServiceRole --targets '{"S3Targets":[{"Path":"s3://path_to_sample_data/"}]}'
```

```
$ aws glue start-crawler --name choice
```

```
$ aws glue get-table --database-name choice --name sample --query Table.StorageDescriptor.Columns --output table
```

- Run the following code on AWS Glue
```
from pyspark.context import SparkContext
from awsglue.context import GlueContext

glueContext = GlueContext(SparkContext.getOrCreate())
dyf_sample = glueContext.create_dynamic_frame.from_catalog(
       database = "choice",
       table_name = "sample")
dyf_sample.printSchema()
```

- Run the following code on AWS Glue
```
dyf_sample_resolved = dyf_sample.resolveChoice(specs = [('key','cast:int')])
dyf_sample_resolved.printSchema()
```

### Enforcing schema

- Run the following code on AWS Glue
```
from pyspark.context import SparkContext
from awsglue.context import GlueContext

glueContext = GlueContext(SparkContext.getOrCreate())
dyf_without_schema = glueContext.create_dynamic_frame_from_options(
    connection_type = "s3", 
    connection_options = {
        "paths": ["s3://awsglue-datasets/examples/us-legislators/all/events.json"]
    }, 
    format = "json"
)
dyf_without_schema.printSchema()
```

- Run the following code on AWS Glue
```
from awsglue.gluetypes import Field, ArrayType, StructType, StringType, IntegerType

dyf_without_schema_tmp = glueContext.create_dynamic_frame_from_options(
    connection_type = "s3", 
    connection_options = {
        "paths": ["s3://awsglue-datasets/examples/us-legislators/all/events.json"]
    }, 
    format = "json"
)

schema = StructType([
    Field("id", StringType()),
    Field("name", StringType()),
    Field("classification", StringType()),
    Field("identifiers", ArrayType(StructType([
            Field("schema", StringType()),
            Field("identifier", IntegerType())
        ])),
    ),
    Field("start_date", IntegerType()),
    Field("end_date", IntegerType()),
    Field("organization_id", StringType()),
])

dyf_with_schema = dyf_without_schema_tmp.with_frame_schema(schema)
dyf_with_schema.printSchema()
```

### Flattening nested schema

- Save the following file as `nested.json`
```
{"count": 2, "entries": [{"id": 1, "values": {"k1": "aaa", "k2": "bbb"}}, {"id": 2, "values": {"k1": "ccc", "k2": "ddd"}}]}
```

```
$ aws s3 cp nested.json s3://path_to_nested_json/
```


- Run the following code on AWS Glue
```
from pyspark.context import SparkContext
from awsglue.context import GlueContext

glueContext = GlueContext(SparkContext.getOrCreate())

dyf = glueContext.create_dynamic_frame_from_options(
    connection_type = "s3",
    connection_options = {"paths": ["s3://path_to_nested_json/"]},
    format = "json"
)
dyf.printSchema()
dyf.toDF().show()
```

- Run the following code on AWS Glue
```
from awsglue.transforms import Relationalize
dfc_root_table_name = "root"
dfc = Relationalize.apply(
    frame = dyf, 
    staging_path = "s3://your-tmp-s3-path/", 
    name = dfc_root_table_name
)
dfc.keys()
```

- Run the following code on AWS Glue
```
dyf_flattened_root = dfc.select(dfc_root_table_name)
dyf_flattened_root.printSchema()
dyf_flattened_root.toDF().show()
```

- Run the following code on AWS Glue
```
dyf_flattened_entries = dfc.select('root_entries')
dyf_flattened_entries.printSchema()
dyf_flattened_entries.toDF().show()
```

- Run the following code on AWS Glue
```
df_flattened_root = dyf_flattened_root.toDF()
df_flattened_entries = dyf_flattened_entries.toDF()

df_joined = df_flattened_root.join(df_flattened_entries)
df_joined.printSchema()
df_joined.show()
```

### Normalizing date and time values

- Run the following code on AWS Glue
```
df_time_string = spark.sql("SELECT '2021-12-25 00:00:00' as timestamp_col") 
df_time_string.printSchema()
df_time_string.show()
```

- Run the following code on AWS Glue
```
from pyspark.sql.functions import to_timestamp, col

df_time_timestamp = df_time_string.withColumn(
    "timestamp_col", 
    to_timestamp(col("timestamp_col"), 'yyyy-MM-dd HH:mm:ss')
)
df_time_timestamp.printSchema()
df_time_timestamp.show()
```

- Run the following code on AWS Glue
```
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue import DynamicFrame

glueContext = GlueContext(SparkContext.getOrCreate())
dyf = DynamicFrame.fromDF(df_time_string, glueContext, "from_df")
```

- Run the following code on AWS Glue
```
mapping = []
for field in dyf.schema():
    if field.name == 'timestamp_col':
        mapping.append((
            field.name, field.dataType.typeName(), 
            field.name, 'timestamp'
        ))
    else:
        mapping.append((
            field.name, field.dataType.typeName(), 
            field.name, field.dataType.typeName()
        ))

dyf = dyf.apply_mapping(mapping)

df_time_timestamp_dyf = dyf.toDF()
df_time_timestamp_dyf.printSchema()
df_time_timestamp_dyf.show()
```

- Run the following code on AWS Glue
```
from pyspark.sql.functions import year, month, dayofmonth

df_time_timestamp_ymd = df_time_timestamp \
    .withColumn('year', year("timestamp_col"))\
    .withColumn('month', month("timestamp_col"))\
    .withColumn('day', dayofmonth("timestamp_col"))
df_time_timestamp_ymd.printSchema()
df_time_timestamp_ymd.show()
```

- Run the following code on AWS Glue
```
def add_timestamp_column(record):
    dt = record["timestamp_col"]
    record["year"] = dt.year
    record["month"] = dt.month
    record["day"] = dt.day
    return record

dyf = dyf.map(add_timestamp_column)
df_time_timestamp_dyf_ymd = dyf.toDF()
df_time_timestamp_dyf_ymd.printSchema()
df_time_timestamp_dyf_ymd.show()
```

### Handling error records

- Run the following code on AWS Glue
```
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

ERROR_RATE_THRESHOLD = 0.2
glue_context = GlueContext(SparkContext.getOrCreate())

dyf = glue_context.create_dynamic_frame.from_options(
    connection_type = "s3", 
    connection_options = {'paths': ['s3://your_input_data_path/']}, 
    format = "csv", 
    format_options={'withHeader': False}
)
dataCount = dyf.count()
errorCount = dyf.errorsCount()
errorRate = errorCount/(dataCount+errorCount)
print(f"error rate: {errorRate}")
if errorRate > ERROR_RATE_THRESHOLD:
    raise Exception(f"error rate {errorRate} exceeded threshold: {ERROR_RATE_THRESHOLD}")

errorDyf = dyf.errorsAsDynamicFrame()
glue_context.write_dynamic_frame_from_options(
    frame=errorDyf,
    connection_type='s3',
    connection_options={'path': 's3://your_error_frame_path/'},
    format='json'
)
```

## De-normalizing tables

- Run the following code on AWS Glue
```
df_product = spark.createDataFrame([
    (11, "Introduction to Cloud", "Ebooks", 15),
    (12, "Best practices on data lakes", "Ebooks", 25),
    (21, "Data Quest", "Video games", 30),
    (22, "Final Shooting", "Video games", 20)
], ['product_id', 'product_name', 'category', 'price'])
df_product.show()
df_product.createOrReplaceTempView("product")
```

- Run the following code on AWS Glue
```
df_customer = spark.createDataFrame([
    ("A103", "Barbara Gordon", "gordon@example.com", "117.835.2584"),
    ("A042", "Rebecca Thompson", "thompson@example.net", "001-469-964-3897x9041"),
    ("A805", "Rachel Gilbert", "gilbert@example.com", "001-510-198-4613x23986"),
    ("A404", "Tanya Fowler", "tanya@example.net", "(067)150-0263")
], ['uid', 'customer_name', 'email', 'phone'])
df_customer.show(truncate=False)
df_customer.createOrReplaceTempView("customer")
```

- Run the following code on AWS Glue
```
df_sales = spark.createDataFrame([
    (21, "A042", "2022-03-30T01:30:00Z"),
    (22, "A805", "2022-04-01T02:00:00Z"),
    (11, "A103", "2022-04-21T11:40:00Z"),
    (12, "A404", "2022-04-28T08:20:00Z")
], ['product_id', 'purchased_by', 'purchased_at'])
df_sales.show(truncate=False)
df_sales.createOrReplaceTempView("sales")
```

- Run the following code on AWS Glue
```
spark.sql("SELECT customer_name, purchased_at FROM sales JOIN customer ON sales.purchased_by=customer.uid WHERE purchased_at LIKE '2022-04%'").show()
```

- Run the following code on AWS Glue
```
df_product_sales = df_product.join(
    df_sales, 
    df_product.product_id == df_sales.product_id
)
df_destination = df_product_sales.join(
    df_customer, 
    df_product_sales.purchased_by == df_customer.uid
)
df_destination.createOrReplaceTempView("destination")
df_destination.select('product_name','category','price','customer_name','email','phone','purchased_at').show()
```

- Run the following code on AWS Glue
```
spark.sql("SELECT product_name,category,price,customer_name,email,phone,purchased_at FROM destination WHERE purchased_at LIKE '2022-04%'").show()
```

## Securing data content

### Masking values

- Run the following code on AWS Glue
```
df_masked = df_destination.withColumn("phone", lit("*"))
df_masked.select('product_name','category','price','customer_name','email','phone','purchased_at').show()
```

- Run the following code on AWS Glue
```
from awsglueml.transforms import EntityDetector

entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    dyf,
    [
        "PERSON_NAME",
        "EMAIL",
        "CREDIT_CARD",
        "IP_ADDRESS",
        "MAC_ADDRESS",
        "PHONE_NUMBER",
        "USA_PASSPORT_NUMBER",
        "USA_SSN",
        "USA_ITIN",
        "BANK_ACCOUNT",
        "USA_DRIVING_LICENSE",
        "USA_HCPCS_CODE",
        "USA_NATIONAL_DRUG_CODE",
        "USA_NATIONAL_PROVIDER_IDENTIFIER",
        "USA_DEA_NUMBER",
        "USA_HEALTH_INSURANCE_CLAIM_NUMBER",
        "USA_MEDICARE_BENEFICIARY_IDENTIFIER",
        "JAPAN_BANK_ACCOUNT",
        "JAPAN_DRIVING_LICENSE",
        "JAPAN_MY_NUMBER",
        "JAPAN_PASSPORT_NUMBER",
        "UK_BANK_ACCOUNT",
        "UK_BANK_SORT_CODE",
        "UK_DRIVING_LICENSE",
        "UK_ELECTORAL_ROLL_NUMBER",
        "UK_NATIONAL_HEALTH_SERVICE_NUMBER",
        "UK_NATIONAL_INSURANCE_NUMBER",
        "UK_PASSPORT_NUMBER",
        "UK_PHONE_NUMBER",
        "UK_UNIQUE_TAXPAYER_REFERENCE_NUMBER",
        "UK_VALUE_ADDED_TAX",
    ],
    1.0,
    0.1,
)

def maskDf(df, keys):
    if not keys:
        return df
    df_to_mask = df.toDF()
    for key in keys:
        df_to_mask = df_to_mask.withColumn(key, lit("*"))
    return DynamicFrame.fromDF(df_to_mask, glueContext, "updated_masked_df")

dyf = maskDf(dyf, list(classified_map.keys()))
```

### Hashing values

- Run the following code on AWS Glue
```
def pii_column_hash(original_cell_value):
    return hashlib.sha256(original_cell_value.encode()).hexdigest()

pii_column_hash_udf = udf(pii_column_hash, StringType())

df_hashed = df_masked.withColumn("email", pii_column_hash_udf("email"))
df_hashed.select('product_name','category','price','customer_name','email','phone','purchased_at').show()
```

- Run the following code on AWS Glue
```
from awsglueml.transforms import EntityDetector

entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    dyf,
    [
        "PERSON_NAME",
        "EMAIL",
        "CREDIT_CARD",
        "IP_ADDRESS",
        "MAC_ADDRESS",
        "PHONE_NUMBER",
        "USA_PASSPORT_NUMBER",
        "USA_SSN",
        "USA_ITIN",
        "BANK_ACCOUNT",
        "USA_DRIVING_LICENSE",
        "USA_HCPCS_CODE",
        "USA_NATIONAL_DRUG_CODE",
        "USA_NATIONAL_PROVIDER_IDENTIFIER",
        "USA_DEA_NUMBER",
        "USA_HEALTH_INSURANCE_CLAIM_NUMBER",
        "USA_MEDICARE_BENEFICIARY_IDENTIFIER",
        "JAPAN_BANK_ACCOUNT",
        "JAPAN_DRIVING_LICENSE",
        "JAPAN_MY_NUMBER",
        "JAPAN_PASSPORT_NUMBER",
        "UK_BANK_ACCOUNT",
        "UK_BANK_SORT_CODE",
        "UK_DRIVING_LICENSE",
        "UK_ELECTORAL_ROLL_NUMBER",
        "UK_NATIONAL_HEALTH_SERVICE_NUMBER",
        "UK_NATIONAL_INSURANCE_NUMBER",
        "UK_PASSPORT_NUMBER",
        "UK_PHONE_NUMBER",
        "UK_UNIQUE_TAXPAYER_REFERENCE_NUMBER",
        "UK_VALUE_ADDED_TAX",
    ],
    1.0,
    0.1,
)

def pii_column_hash(original_cell_value):
    return hashlib.sha256(original_cell_value.encode()).hexdigest()

pii_column_hash_udf = udf(pii_column_hash, StringType())

def hashDf(df, keys):
    if not keys:
        return df
    df_to_hash = df.toDF()
    for key in keys:
        df_to_hash = df_to_hash.withColumn(key, pii_column_hash_udf(key))
    return DynamicFrame.fromDF(df_to_hash, glueContext, "updated_hashed_df")

dyf = hashDf(dyf, list(classified_map.keys()))
```

## Managing data quality

- Run the following code on AWS Glue
```
import pydeequ
from pyspark.sql import SparkSession
spark = (SparkSession
    .builder
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    .getOrCreate())
df = spark.createDataFrame([
    (1, "Product A", "awesome thing.", "high", 2),
    (2, "Product B", "available at http://producta.example.com", None, 0),
    (3, None, None, "medium", 6),
    (4, "Product D", "checkout https://productd.example.org", "low", 10),
    (5, "Product E", None, "high", 18)
], ['id', 'productName', 'description', 'priority', 'numViews'])
```

- Run the following code on AWS Glue
```
from pydeequ.analyzers import *
analysisResult = AnalysisRunner(spark) \
    .onData(df) \
    .addAnalyzer(Size()) \
    .addAnalyzer(Completeness("id")) \
    .addAnalyzer(Completeness("productName")) \
    .addAnalyzer(Maximum("numViews")) \
    .addAnalyzer(Mean("numViews")) \
    .addAnalyzer(Minimum("numViews")) \
    .run()
analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
analysisResult_df.show()
```

- Run the following code on AWS Glue
```
from pydeequ.checks import *
from pydeequ.verification import *

check = Check(spark, CheckLevel.Warning, "Review Check")

checkResult = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(
        # we expect 5 row
        check.hasSize(lambda x: x == 5) \
        # should never be NULL
        .isComplete("id") \
        # should not contain duplicates
        .isUnique("id") \
        # should never be NULL
        .isComplete("productName") \
        # should only contain the values "high", "medium", and "low"
        .isContainedIn("priority", ["high", "medium", "low"]) \
        # should not contain negative values
        .isNonNegative("numViews") \
        # at least half of the descriptions should contain a url
        .containsURL("description", lambda x: x >= 0.5) \
        # half of the items should have less than 10 views
        .hasApproxQuantile("numViews", ".5", lambda x: x <= 10)) \
    .run()

checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.show()
```
