from datetime import datetime, timedelta
from os.path import join
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
import logging
from dotenv import load_dotenv
from os import getenv
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import glob


load_dotenv('/workspace/datapipeline/credentials_aws.txt')


s3_client = boto3.client(
    's3',
    aws_access_key_id=getenv('access_key_id'),
    aws_secret_access_key=getenv('secret_access_key'),
    region_name='us-east-1'
)


def create_bucket_landing(bucket_name='dados-landing'):
    try:
      s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
        
    return True

def create_bucket_processing(bucket_name='dados-processing'):
    try:
      s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
        
    return True

def create_bucket_curated(bucket_name='dados-curated'):
    try:
      s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
        
    return True

csv_files = glob.glob("/workspace/datapipeline/data/*.csv")

for file in csv_files:

    def upload_object(file_name=file, bucket='landing', object_name=None):

        if object_name is None:
            object_name = file_name

        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        
        return True

ARGS = {
    "owner": "lais",
    "depends_on_past": False,
    "start_date": datetime(2022,2,24)
    
}



dag = DAG( dag_id='ETL', default_args=ARGS, schedule_interval=None)



create_landing = PythonOperator(

    task_id='create_bucket-landing',
    python_callable=create_bucket_landing,
    dag = dag

)

create_processing = PythonOperator(

    task_id='create_bucket-processing',
    python_callable=create_bucket_processing,
    dag = dag

)


create_curated = PythonOperator(

    task_id='create_bucket-curated',
    python_callable=create_bucket_curated,
    dag = dag

)


upload_data_landing = PythonOperator(

    task_id='upload_data_landing',
    python_callable=upload_object,
    dag = dag

)

spark_job = SparkSubmitOperator(
    application="/workspace/datapipeline/spark.py", 
    task_id="data_transformation_spark",
    dag = dag
)
    

create_landing >> create_processing >> create_curated >> upload_data_landing
upload_data_landing >> spark_job
