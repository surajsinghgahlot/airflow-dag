from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Suraj',
    'type': 'DockerOperator',
}

dag = DAG(
    dag_id="base_docker_operator",
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

hello_world_docker = DockerOperator(
    task_id='hello_world_docker',
    image='hello-world:latest',
    container_name='hello_world',
    api_version='auto',
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge"
)

run_this_first >> hello_world_docker