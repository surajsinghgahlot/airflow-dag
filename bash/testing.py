from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'suraj',
    'type': 'BashOperator',
}

dag = DAG(
    dag_id='testing',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['for testing purpose']
)

run_this_first = DummyOperator(
    task_id='run_this_first',
    dag=dag,
)

now_run_spark_job = BashOperator(
    task_id='now_run_spark_job',
    bash_command='ls /dags ',
    dag=dag,
)

run_this_first >> now_run_spark_job
