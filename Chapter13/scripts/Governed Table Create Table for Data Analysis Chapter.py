import boto3
import sys
from awsglue.utils import getResolvedOptions

client = boto3.client('glue')

args = getResolvedOptions(sys.argv, ['TARGET_BUCKET'])

client.create_table(DatabaseName='chapter-data-analysis-glue-database', TableInput={
     'Name': 'employees_governed_table',
     'Description': 'people table for manual entries.',
     'StorageDescriptor': {
        'Columns': [
             {'Name': 'emp_no', 'Type': 'int'},
             {'Name': 'name', 'Type': 'string'},
             {'Name': 'department', 'Type': 'string'},
             {'Name': 'city', 'Type': 'string'},
             {'Name': 'salary', 'Type': 'int'}
             ],
        'Location': "s3://"+args['TARGET_BUCKET']+"/employees_governed_table/",
        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
        'Compressed': False,
        'NumberOfBuckets': 0,
        'SerdeInfo': {'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
        'Parameters': {}
        },
        'SortColumns': [],
        'StoredAsSubDirectories': False
      },
      'TableType': 'GOVERNED',
      'Parameters': {'classification': 'parquet',
                'lakeformation.aso.status': 'true'}
        }
    )