from dotenv import load_dotenv
from pyspark.sql import SparkSession
import os

load_dotenv(f"{os.getcwd()}/new_transformer/local.env")

def get_raw_data_in_batch(spark,source,table,size):

    source = source.upper()

    db_host = os.environ.get(f"PG_HOST_{source}")
    db_port = os.environ.get("PG_PORT")
    db_name = os.environ.get(f"PG_DB_{source}")
    db_password = os.environ.get("PG_PASSWORD")
    db_user = os.environ.get("PG_USERNAME")

    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}") \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .option("query",f"select * from {table} where reclean_status is null limit {size}") \
        .load()
        # .option('fetchsize',f"{size}") \

    return df

if __name__ == "__main__":

    table = 'mh_gom_rd_deeds_pune.pune_deeds_e_filing'
    size = 1000
    source = 'RAW'

    spark = SparkSession \
        .builder \
        .appName("teal-data-transformer") \
        .getOrCreate()

    df = get_raw_data_in_batch(spark, source, table, size)
    df.show()
        # .config("spark.jars", "postgresql-42.3.3.jar") \
