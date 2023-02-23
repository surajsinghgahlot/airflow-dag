from datetime import timedelta
from airflow import DAG
from my_script import create_bucket
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

python_task = PythonOperator(
    task_id='python_task',
    python_callable=create_bucket,
    dag=dag
)

run_this_first >> python_task