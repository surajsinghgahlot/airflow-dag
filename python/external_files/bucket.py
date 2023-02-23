from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'suraj',
    'type': 'PythonOperator',
}

dag = DAG(
    dag_id="external_files",
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

python_task = BashOperator(
    task_id='python_task',
    bash_command='python create_bucket.py',
    dag=dag
)

run_this_first >> python_task