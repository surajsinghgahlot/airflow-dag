from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

import numpy as np


args = {
    'owner': 'suraj',
    'type': 'testing',
}

dag = DAG(
    dag_id="example_python_operator",
    schedule=None,
    default_args=args,
    start_date=days_ago(2),
    catchup=False,
    tags=["example_python_operator"],
)

@task()
def print_array():
    """Print Numpy array."""
    a = np.arange(15).reshape(3, 5)
    print(a)
    return a

print_array()