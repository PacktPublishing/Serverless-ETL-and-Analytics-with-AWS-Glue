from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import awswrangler as wr
import boto3
import sys
import pandas as pd
from awsglue.utils import getResolvedOptions

client = boto3.client('glue')
args = getResolvedOptions(sys.argv, ['TARGET_BUCKET'])

bootstrap_servers = client.get_connection(Name='chapter-data-analysis-msk-connection')['Connection']['ConnectionProperties']['KAFKA_BOOTSTRAP_SERVERS'].split(",")
admin = KafkaAdminClient(security_protocol="SSL", bootstrap_servers=bootstrap_servers, client_id='chapter-data-analysis-add-data')
topic='chapter-data-analysis'
topic_list = []
topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=2))
admin.create_topics(new_topics=topic_list, validate_only=False)
producer = KafkaProducer(security_protocol="SSL", bootstrap_servers=bootstrap_servers, value_serializer=lambda x:x.encode('utf-8'))
df = wr.s3.read_parquet(path='s3://'+args['TARGET_BUCKET']+'/seed_data/employees/')
now = pd.Timestamp.today().timestamp()
df['data'] = df.assign(record_creation_time=now).to_json(orient='records', lines=True).splitlines()
for row in df['data']:
    print(row)
    producer.send(topic,row)  
    producer.flush()
producer.close()