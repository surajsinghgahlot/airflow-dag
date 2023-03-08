from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
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

# run_this_first = DummyOperator(
#     task_id='run_this_first',
#     dag=dag
# )

# base_spark_job = SparkSubmitOperator(
# 	application ='./basic_submit_job.py',
# 	conn_id= 'spark_local', 
# 	task_id='base_spark_job', 
# 	dag=dag
# )

base_bash_job_1 = BashOperator(
    task_id='base_bash_job_1',
    bash_command='ls',
    dag=dag,
)

base_bash_job_2 = BashOperator(
    task_id='base_bash_job_2',
    bash_command='pwd',
    dag=dag,
)

base_bash_job_3 = BashOperator(
    task_id='base_bash_job_3',
    bash_command='whereis spark',
    dag=dag,
)

base_bash_job_1 >> base_bash_job_2 >> base_bash_job_3