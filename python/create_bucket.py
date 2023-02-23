#=================================================================================================================
import boto3
import sys
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'suraj',
    'type': 'PythonOperator',
}

dag = DAG(
    dag_id="aws_bucket",
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=["s3 bucket"]
)

run_this_first = DummyOperator(
    task_id='run_this_first',
    dag=dag
)

ACCESS_KEY=Variable.get("ACCESS_KEY")
SECRET_KEY=Variable.get("SECRET_KEY")
REGION="ap-south-1"
BUCKET_NAME="ss14suraj1234"

#Console Login:
def get_s3_client():
    session=boto3.session.Session(aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,region_name=REGION)
    s3_client=session.client('s3')
    return s3_client

#Code:
def create_bucket():
    s3_response=get_s3_client()
    try:
        s3_response.create_bucket(Bucket=BUCKET_NAME,CreateBucketConfiguration={'LocationConstraint': REGION})
        print(BUCKET_NAME,"is created")
    except Exception as e:
        if e.response['Error']['Code']=="BucketAlreadyOwnedByYou":
            print("Bucket Already Owned By You")
        elif e.response['Error']['Code']=="BucketAlreadyExists":
            print("Bucket Already Exist")
        elif e.response['Error']['Code']=="InvalidBucketName":
            print("Bucket Name Is Invalid")
        else:
            print(e)
    return None


python_task = PythonOperator(
    task_id='python_task',
    python_callable=create_bucket,
    dag=dag
)

run_this_first >> python_task