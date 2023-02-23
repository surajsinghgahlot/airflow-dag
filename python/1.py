from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import numpy as np


args = {
    'owner': 'suraj',
    'type': 'testing',
}

dag = DAG(
    dag_id="example_python_operator",
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=["example_python_operator"]
)

run_this_first = DummyOperator(
    task_id='run_this_first',
    dag=dag,
)

def my_func():
  print('welcome to Dezyre')
  return 'welcome to Dezyre'

python_task = PythonOperator(task_id='python_task', python_callable=my_func, dag=dag)

run_this_first >> python_task