### We will name the DAG as edgeToEMRandS3
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id='testing',
#     default_args=args,
#     schedule_interval=None,
#     start_date=days_ago(2),
#     dagrun_timeout=timedelta(minutes=60),
#     tags=['somethingForYouToFindYourDAG']
#     #params={"example_key": "example_value"},
)

run_this_first = DummyOperator(
    task_id='run_this_first',
    dag=dag,
)

now_run_spark_job = BashOperator(
    task_id='now_run_spark_job',
    bash_command='bash /home/ec2-user/airflow/scripts/testEMRtoS3Conn_pyWrapper.sh ',
    dag=dag,
)

run_this_first >> now_run_spark_job
