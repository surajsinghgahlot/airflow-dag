from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'PythonOperator',
}

dag = DAG(
    dag_id="base_python_operator",
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

def my_func():
    print('welcome to Dezyre')
    return 'welcome to Dezyre'

base_python_job = PythonOperator(
    task_id='base_python_job', 
    python_callable=my_func, 
    dag=dag
)

run_this_first >> base_python_job