from pyspark.sql import SparkSession
# import findspark
# findspark.init('/home/darshan/Downloads/spark-3.2.1-bin-hadoop3.2')

def get_spark_context():

    spark = SparkSession \
        .builder \
        .appName("teal-data-transformer") \
        .getOrCreate()

    return spark