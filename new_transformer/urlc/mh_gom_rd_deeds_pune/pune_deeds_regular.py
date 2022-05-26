from indictrans import Transliterator
from new_transformer.utils import pg_reader
from new_transformer.utils import spark
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from new_transformer.urlc.mh_gom_rd_deeds_pune import raw_to_clean_pipeline_definitions
from time import perf_counter
from new_transformer.utils.pg_updater import update_batch
from pyspark.sql.functions import lit
from new_transformer.tagger.tagger import Tagger
from pyspark.sql.functions import concat,col

SOURCE_TABLE = 'mh_gom_rd_deeds_pune.reg_test'
BATCH_SIZE = 100
SOURCE_DB_TYPE = 'RAW'

COLUMNS_TO_FOR_RAW_UPDATE = ['raw_hash', 'reclean_status', 'source_table', 'clean_hash']

COLUMNS_TO_DROP_FOR_CLEAN = [
    "raw_year",
]

HASH_KEYS =  ["district_raw", "sro_raw", "year", "document_no", "source_table"],

COLUMNS_TO_DROP_FOR_CLEAN = [
    'html_text',
    'raw_year',
    'last_downloaded',
    'index_html',
    'sro_name',
    'seller_name',
    'reg_date',
    'purchaser_name',
    'prop_description',
    'status',
    'clean_status_string',
    'doc_name',
    'clean_status',
    'raw_first_party_names',
    'raw_second_party_names',
    "reclean_meta",
    "reclean_status",
]

RAW_COLUMNS_TO_RENAME = {
    "district" : "district_raw",
    "sro" : "sro_raw",
    "doc_no" : "document_no",
    "year" : "raw_year",
    "doc_name" : "deed_type_raw",
    "sro_code" : "sro_code"
}

def fetch_raw_rows_in_batch(spark_context, source_db_type, source_table, batch_size):

    df = pg_reader.get_raw_data_in_batch(spark_context, source_db_type, source_table, batch_size)

    for column in RAW_COLUMNS_TO_RENAME:
        df = df.withColumnRenamed(column, RAW_COLUMNS_TO_RENAME[column])

    df = df.withColumn('source_table', lit(source_table))

    return df


def execute_pipeline_definitions(df, pipeline_definitions):
    for attribute in pipeline_definitions:
        config = pipeline_definitions[attribute]
        source_column = config['source_column']
        destination_column = config['destination_column']
        function = config['function']
        df = df.withColumn(destination_column, function(col(source_column)))

    return df


def main():
    start_time = perf_counter()
    spark_context = spark.get_spark_context()
    pipeline_definitions = raw_to_clean_pipeline_definitions()

    df = fetch_raw_rows_in_batch(spark_context, SOURCE_DB_TYPE, SOURCE_TABLE, BATCH_SIZE)
    # df.printSchema()
    
    clean_df_for_write = execute_pipeline_definitions(df, pipeline_definitions)
    clean_df_for_write = clean_df_for_write.drop(*COLUMNS_TO_DROP_FOR_CLEAN)
    clean_df_for_write.printSchema()

    # clean_df_for_write = clean_df_for_write.drop("clean_hash")
    # clean_df_for_write.rdd.coalesce(1).foreachPartition(write_batch) # uncomment this to see the writes

    # raw update
    # raw_df_for_update.rdd.coalesce(1).foreachPartition(update_batch)

    # clean_df_for_write.show(100)
    end_time = perf_counter()
    print("Elapsed time during the whole program in seconds:",
          end_time - start_time)


if __name__ == "__main__":
    main()