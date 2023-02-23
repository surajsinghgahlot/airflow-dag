from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

args = {
    'owner': 'suraj',
}

dag = DAG(
    dag_id='testing',
)

run_this_first = DummyOperator(
    task_id='run_this_first',
    dag=dag,
)

now_run_spark_job = BashOperator(
    task_id='now_run_spark_job',
    bash_command='bash /home/ec2-user/airflow/scripts/testEMRtoS3Conn_pyWrapper.sh ',
    dag=dag,
)

run_this_first >> now_run_spark_job
