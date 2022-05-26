from pyspark.sql import SparkSession


def get_spark_context():

    spark = SparkSession \
        .builder \
        .appName("teal-data-transformer") \
        .getOrCreate()

    return spark