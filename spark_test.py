from pyspark.sql import SparkSession
import sys

def init_spark():
    spark = SparkSession.builder\
    .appName("sample_py_app")\
    .enableHiveSupport()\
    .getOrCreate()
    return spark

def list_databases(spark):
    # list the database under lakehouse catalog
    spark.sql(f"show databases").show(truncate=False)

def main():
    try:
        spark = init_spark()
        list_databases(spark)

    finally:
        spark.stop()

if __name__ == '__main__':
  exit_code = 0
  try:
      main()
  except Exception as e:
      print(e)
      exit_code = 1
  sys.exit(exit_code)
