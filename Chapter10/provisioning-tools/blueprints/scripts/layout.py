from awsglue.blueprint.workflow import Workflow, Entities
from awsglue.blueprint.job import Job
from awsglue.blueprint.crawler import Crawler
import boto3
s3c = boto3.client('s3')


def set_script_location(script_path: str, script_name: str) -> str:
    if script_path.endswith('/'):
        script_path = script_path[:-1]
    return f'{script_path}/{script_name}'

def upload_scripts(client, script_location: str, script_names: list) -> None:
    def _upload_script(client, bucket: str, path_wo_slash: str, script_name: str) -> None:
        with open(f'{script_name}', 'rb') as f:
            client.upload_fileobj(f, bucket, f'{path_wo_slash}/{script_name}')  
    
    if script_location.endswith('/'):
        script_location = script_location[:-1]
    
    bucket = script_location.split('//')[1].split("/", 1)[0]
    path_wo_slash = script_location.split('//')[1].split("/", 1)[1:][0]
    for script_name in script_names:
        _upload_script(client=client, bucket=bucket, path_wo_slash=path_wo_slash, script_name=script_name)
        

'''
The main part of generating the workflow based on the definition in this function.
'''
def generate_layout(user_params, system_params):
    # Upload job scripts to the specified script location
    upload_scripts(
        client=s3c, 
        script_location=user_params['ScriptLocation'], 
        script_names=['ch10_5_example_bp_partitioning.py', 'ch10_5_example_bp_gen_report.py'])

    # Sales Crawler
    sales_crawler = Crawler(
        Name='ch10_5_example_bp',
        Role=user_params['GlueCrawlerRoleName'],
        DatabaseName=user_params['DatabaseName'],
        TablePrefix='ch10_5_example_bp_',
        Targets={"S3Targets": [{"Path": user_params['SalesDataLocation']}]}
    )

    # Partitioning job
    partitioning_job = Job(
        Name="ch10_5_example_bp_partitioning",
        Command={
            "Name": "glueetl", 
            "ScriptLocation": set_script_location(script_path=user_params['ScriptLocation'], script_name='ch10_5_example_bp_partitioning.py'),
            "PythonVersion": "3"},
        Role=user_params['GlueJobRoleName'],
        WorkerType="G.1X",
        NumberOfWorkers=5,
        DefaultArguments={
            '--enable-glue-datacatalog': '',
            '--enable-continuous-cloudwatch-log': 'true',
            '--enable-metrics': '',
            '--job-language': 'python',
            '--extra-jars': 's3://crawler-public/json/serde/json-serde.jar'},
        GlueVersion="4.0",
        DependsOn={sales_crawler: "SUCCEEDED"})

    # gen_report_job
    gen_report_job = Job(
        Name="ch10_5_example_bp_gen_report",
        Command={
            "Name": "glueetl", 
            "ScriptLocation": set_script_location(script_path=user_params['ScriptLocation'], script_name='ch10_5_example_bp_gen_report.py'),
            "PythonVersion": "3"},
        Role=user_params['GlueJobRoleName'],
        WorkerType="G.1X",
        NumberOfWorkers=5,
        DefaultArguments={
            '--enable-glue-datacatalog': '',
            '--enable-continuous-cloudwatch-log': 'true',
            '--enable-metrics': '',
            '--job-language': 'python'},
        GlueVersion="4.0",
        DependsOn={partitioning_job: "SUCCEEDED"}
    )

    return Workflow(
        Name=user_params['WorkflowName'], 
        DefaultRunProperties={
            'datalake_location': user_params['DataLakeLocation'],
            'database': user_params['DatabaseName'],
            'table': 'ch10_5_example_bp_sales',
            'report_year': user_params['ReportYear']
        },
        Entities=Entities(
            Jobs=[partitioning_job, gen_report_job],
            Crawlers=[sales_crawler]))