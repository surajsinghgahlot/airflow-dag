from pyspark.sql import SparkSession
from pathlib import Path
import os
from pyspark.sql.types import StringType, StructType, StructField

def open_session():
    spark = SparkSession.builder.appName('SparkCode').getOrCreate()
    return spark

def create_directory(path_dir):
    try:
        Path(path_dir).mkdir(parents=True, exist_ok=True)
        os.chmod(path_dir, 0o777)
        print("Directory Created", path_dir)
    except Exception as e:
        print("Error in Creating Directory", path_dir)
        print(e)
        raise Exception(e)

def create_file(index):
    schema = StructType([
        StructField('Number', StringType(), True),
        StructField('Number * 2', StringType(), True),
    ])
    df = session.createDataFrame(
        [
            (index, index * 2),
            (index, index * 2),
            (index, index * 2),
            (index, index * 2),
        ], schema)
    df.show(truncate=False)
    dir_name = "DF_DIR"
    create_directory(dir_name)
    file_name = f"spark_result_{i}.csv"
    df.toPandas().to_csv(dir_name + "/" + file_name, header=True, index=False)

if __name__ == "__main__":
    for i in range(1, 5):
        session = open_session()
        create_file(i)
        session.stop()