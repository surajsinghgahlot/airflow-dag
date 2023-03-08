import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('abc').getOrCreate()
df = spark.read.csv("python.csv", header=True)
df.printSchema()
print(df.show())
spark.stop