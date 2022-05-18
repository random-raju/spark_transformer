from traceback import print_tb
from new_transformer.urlc.mh_gom_rd_deeds_pune import pune_deeds_regular
from new_transformer.utils import pg_reader
from new_transformer.utils import spark
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from new_transformer.urlc.mh_gom_rd_deeds_pune import raw_to_clean_pipeline_definitions
from time import perf_counter
from new_transformer.utils.pg_updater import update_batch
from new_transformer.utils.mongo_writer import write_batch
from pyspark.sql.functions import lit
from new_transformer.tagger.tagger import Tagger


def main():
    print("Spark Rocks darshan")

if __name__== '__main__':
    print("testing")