from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.utils.dates import days_ago

args = {
    'owner': 'SparkOperator',
}

dag = DAG(
    dag_id="base_spark_operator",
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

base_spark_job = SparkSubmitOperator(
	application ='./basic_submit_job.py',
    env_vars={'PATH': '/bin:/usr/bin:/usr/local/bin'},
	conn_id= 'spark_local', 
	task_id='base_spark_job', 
	dag=dag
)

run_this_first >> base_spark_job