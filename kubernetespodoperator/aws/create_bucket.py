from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'KubernetesPodOperator',
}

dag = DAG(
    dag_id="create_bucket",
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

create_s3_bucket = KubernetesPodOperator(
    name='create_s3_bucket',
    namespace="airflow",
    task_id='create_s3_bucket',
    image='ss14suraj/boto3:s3',
    cmds=["python"],
    arguments=["create_bucket.py"],
    # image_pull_secrets=[k8s.V1LocalObjectReference("imagescretsname")],
    env_vars={
        "AWS_ACCESS_KEY": Variable.get("ACCESS_KEY"),
        "AWS_SECRET_KEY": Variable.get("SECRET_KEY"),
        "AWS_REGION": "ap-south-1",
        "AWS_SOURCE_BUCKET": "ss14suraj1234"
    },
    get_logs=True,
    in_cluster=True,
    is_delete_operator_pod=False,
)

run_this_first >> create_s3_bucket