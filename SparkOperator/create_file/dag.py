from datetime import timedelta
import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.utils.dates import days_ago

os.environ["PYSPARK_PYTHON"]="/opt/bitnami/airflow/venv/bin/python3.9"
os.environ["JAVA_HOME"]="/usr"

args = {
    'owner': 'SparkOperator',
}

dag = DAG(
    dag_id="create_file",
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=["for testing purpose"]
)

run_this_first = DummyOperator(
    task_id='run_this_first',
    dag=dag
)

create_file = SparkSubmitOperator(
	application ='/opt/bitnami/airflow/dags/git_airflow-dag/SparkOperator/create_file/spark_job.py',
	conn_id= 'spark_default',
	task_id='create_file',
	dag=dag
)

run_this_first >> create_file