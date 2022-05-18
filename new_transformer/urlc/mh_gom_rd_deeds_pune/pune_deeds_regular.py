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

SOURCE_TABLE = 'mh_gom_rd_deeds_pune.reg_test'
BATCH_SIZE = 100
SOURCE_DB_TYPE = 'RAW'

COLUMNS_TO_FOR_RAW_UPDATE = ['raw_hash', 'reclean_status', 'source_table', 'clean_hash']

COLUMNS_TO_DROP_FOR_CLEAN = [
    'html_text',
    'reclean_meta',
    'last_downloaded',
    'index_html',
    'sro_name',
    'seller_name',
    'reg_date',
    'purchaser_name',
    'prop_description',
    'sro_code',
    'status',
    'clean_status_string',
    # 'doc_name',
    'clean_status',
    'raw_first_party_names',
    'raw_second_party_names'
]


def fetch_raw_rows_in_batch(spark_context, source_db_type, source_table, batch_size):
    df = pg_reader.get_raw_data_in_batch(spark_context, source_db_type, source_table, batch_size)

    df = df.withColumnRenamed("district", "raw_district")
    df = df.withColumnRenamed("sro", "raw_sro")
    df = df.withColumnRenamed("year", "raw_year")
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

    clean_df_for_write = execute_pipeline_definitions(df, pipeline_definitions)
    clean_df_for_write.show()
    # df.show()
    # clean_df_for_write.show()
    # print(2)
    # raw_df_for_update = clean_df_for_write.select('raw_hash','reclean_status','source_table')
    # print(3)

    # # clean/mongo db write
    # clean_df_for_write = clean_df_for_write.drop(*COLUMNS_TO_DROP_FOR_CLEAN)
    # clean_df_for_write = clean_df_for_write.select(sorted(clean_df_for_write.columns))
    # # clean_df_for_write.printSchema()
    # print(4)

    # Tag address, first party & second party names
    # tagger_fp_name = Tagger(clean_df_for_write.toPandas(), 'name', to_tag_column='first_party_names')
    # tagger_fp_name_df = tagger_fp_name.main()
    # tagger_fp_name_df.rename(columns={'data_dict': 'tagged_entity_first_party'}, inplace=True)
    print(5)

    # tagger_fp_name_df.show()
    # schema_fp = StructType(
    #     [StructField("clean_hash", StringType(), True), StructField("tagged_entity_first_party", StringType(), True)])

    # tagger_fp_name_sdf = spark_context.createDataFrame(tagger_fp_name_df, schema=schema_fp)
    # tagger_fp_name_sdf.show()

    # tagger_sp_name = Tagger(clean_df_for_write.toPandas(), 'name', to_tag_column='second_party_names')
    print(6)
    # tagger_sp_name_df = tagger_sp_name.main()
    # tagger_sp_name_df.rename(columns={'data_dict': 'tagged_entity_second_party'}, inplace=True)

    # schema_sp = StructType(
    #     [StructField("clean_hash", StringType(), True), StructField("tagged_entity_second_party", StringType(), True)])

    # tagger_sp_name_sdf = spark_context.createDataFrame(tagger_sp_name_df, schema=schema_sp)
    # tagger_sp_name_sdf.show()

    # tagger_address = Tagger(clean_df_for_write.toPandas(), 'address', district='district', to_tag_column='address')
    # tagger_address_df = tagger_address.main()
    print(7)

    # schema_add = StructType(
    #     [StructField("clean_hash", StringType(), True), StructField("tagged_locality_data", StringType(), True),
    #      StructField("tagged_project_data", StringType(), True), StructField("valid_project", StringType(), True)])

    # tagger_address_sdf = spark_context.createDataFrame(tagger_address_df, schema=schema_add)
    # tagger_address_sdf.show()
    print(8)

    # clean_df_for_write = clean_df_for_write.withColumnRenamed("clean_hash", "_id")

    # clean_df_for_write = clean_df_for_write.join(tagger_fp_name_sdf,
    #                                              clean_df_for_write._id == tagger_fp_name_sdf.clean_hash, "left"). \
    #     join(tagger_sp_name_sdf, clean_df_for_write._id == tagger_sp_name_sdf.clean_hash, "left"). \
    #     join(tagger_address_sdf, clean_df_for_write._id == tagger_address_sdf.clean_hash, "left")

    # clean_df_for_write = clean_df_for_write.drop("clean_hash")
    # clean_df_for_write = clean_df_for_write.select(sorted(clean_df_for_write.columns))
    print('Okay 8')
    # clean_df_for_write.rdd.coalesce(1).foreachPartition(write_batch) # uncomment this to see the writes
    print(9)

    # add data_quality checks and derive reclean_meta values

    # raw update
    # raw_df_for_update.rdd.coalesce(1).foreachPartition(update_batch)
    print(10)

    # clean_df_for_write.show(100)
    # clean_df_for_write.printSchema()
    end_time = perf_counter()
    print("Elapsed time during the whole program in seconds:",
          end_time - start_time)
    print(11)


if __name__ == "__main__":
    main()