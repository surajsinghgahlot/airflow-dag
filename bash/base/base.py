from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'BashOperator',
}

dag = DAG(
    dag_id='base_bash_operator',
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

base_bash_job = BashOperator(
    task_id='base_bash_job',
    bash_command='ls ./bash.sh',
    dag=dag,
)

run_this_first >> base_bash_job
