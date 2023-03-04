from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Suraj',
    'type': 'KubernetesPodOperator',
}

dag = DAG(
    dag_id="base_kubernetes_operator",
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

hello_world_kubernetes = KubernetesPodOperator(
    task_id='hello_world_kubernetes',
    image='hello-world:latest',
    container_name='hello_world',
)

run_this_first >> hello_world_kubernetes