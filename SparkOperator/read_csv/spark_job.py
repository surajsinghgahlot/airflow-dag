import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('abc').getOrCreate()
df = spark.read.csv("/opt/bitnami/airflow/dags/git_airflow-dag/SparkOperator/read_csv/python.csv", header=True)
df.printSchema()
print(df.show())
spark.stop