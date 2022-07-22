from datetime import timedelta  
import airflow  
from airflow import DAG  
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator



default_args = {  
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

dag = DAG(  
    dag_id='ch10_3_example_workflow_mwaa',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 3 * * *'
)

glue_crawler_sales = AwsGlueCrawlerOperator(
    task_id="sales_crawl",
    config={"Name": "ch10_3_example_workflow"},
    poll_interval=15,
    dag=dag)

glue_job_gen_report = AwsGlueJobOperator(  
    task_id="gen_report",  
    job_name='ch10_3_example_workflow_gen_report',  
    script_args={
        "--datalake_location": "s3://<your-bucket-and-path>",
        "--database": "<your-database>",
        "--table": "example_workflow_mwaa_parquet",
        "--report_year": "2021"
        },
    dag=dag) 
#     iam_role_name='<Your Glue Job IAM Role name>',  
glue_crawler_sales >> glue_job_gen_report
