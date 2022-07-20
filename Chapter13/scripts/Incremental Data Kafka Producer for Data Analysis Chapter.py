from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
import awswrangler as wr
import boto3
import pandas as pd

client = boto3.client('glue')
bootstrap_servers = client.get_connection(Name='chapter-data-analysis-msk-connection')['Connection']['ConnectionProperties']['KAFKA_BOOTSTRAP_SERVERS'].split(",")
topic='chapter-data-analysis'
producer = KafkaProducer(security_protocol="SSL", bootstrap_servers=bootstrap_servers, value_serializer=lambda x:x.encode('utf-8'))
now = pd.Timestamp.today().timestamp()
producer.send(topic,dumps({"emp_no": 3,"name": "Jeff","department": "Finance","city": "Cincinnati","salary": 70000,"record_creation_time":now}))  
producer.flush()
producer.close()