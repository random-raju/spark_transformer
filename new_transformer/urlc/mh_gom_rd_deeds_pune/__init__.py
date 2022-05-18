from pyspark.sql.functions import udf
from new_transformer.general_transformer.entity_mapper import EntityMapper
from new_transformer.address_transformer.address_translator import AddressTranslator
from pyspark.sql.types import StringType, DateType, IntegerType , FloatType, ArrayType, TimestampType
from new_transformer.urlc.mh_gom_rd_deeds_pune import html_parser
from new_transformer.names_transformer.names_translator import NamesTranslator
from new_transformer.utils import general
from new_transformer.address_transformer.address_parser import AddressParser

def raw_to_clean_pipeline_definitions():
    print("Okay")
    raw_deed_mapper = EntityMapper(mapper_type="deed", languages=["english"]).map_entity
    deed_type_mapper = udf(lambda text : raw_deed_mapper(text), StringType())

    # raw_district_mapper = EntityMapper(mapper_type="district", languages=["marathi"]).map_entity
    # district_mapper = udf(lambda text : raw_district_mapper(text), StringType())

    # raw_sro_mapping = EntityMapper(mapper_type="sro", languages=["english"]).map_entity
    # sro_mapper = udf(lambda text : raw_sro_mapping(text), StringType())

    # get_deeds_info_from_html_text = udf(lambda text : html_parser.get_html_as_str(text), StringType())

    # parse_prop_desc = udf(lambda text : html_parser.parse_prop_desc(text), StringType())

    # parse_area = udf(lambda text : html_parser.parse_area(text), StringType())

    # parse_first_party = udf(lambda text : html_parser.parse_first_party(text), StringType())
    # parse_first_names = udf(lambda text : html_parser.parse_party_names(text), StringType())
    # parse_first_pans = udf(lambda text : html_parser.parse_party_pan(text), ArrayType(StringType()))

    # parse_second_party = udf(lambda text : html_parser.parse_second_party(text), StringType())
    # parse_second_names = udf(lambda text : html_parser.parse_party_names(text), StringType())
    # parse_second_pans = udf(lambda text : html_parser.parse_party_pan(text), ArrayType(StringType()))

    # parse_mortgage_date = udf(lambda text : html_parser.parse_mortgage_date(text), TimestampType())
    # parse_date_of_filing = udf(lambda text : html_parser.parse_date_of_filing(text), TimestampType())
    # parse_date_of_execution = udf(lambda text : html_parser.parse_date_of_execution(text), TimestampType())
    # parse_reg_date = udf(lambda text : html_parser.parse_reg_date(text), TimestampType())
    # parse_date_of_submission = udf(lambda text : html_parser.parse_date_of_submission(text), TimestampType())

    # parse_stamp_duty = udf(lambda text : html_parser.parse_stamp_duty(text), FloatType())
    # parse_filing_amount = udf(lambda text : html_parser.parse_filing_amount(text), FloatType())
    # parse_compensation = udf(lambda text : html_parser.parse_compensation(text), FloatType())
    # parse_market_price = udf(lambda text : html_parser.parse_market_price(text), FloatType())
    # parse_reg_fee = udf(lambda text : html_parser.parse_reg_fee(text), FloatType())
    # parse_loan_amount = udf(lambda text : html_parser.parse_loan_amount(text), FloatType())

    # address_translator = AddressTranslator(["marathi"])
    # get_address_mapped = address_translator.get_address_mapped
    # address_mapper = udf(lambda x: get_address_mapped(x), StringType())

    # address_translator = AddressTranslator(["marathi"])
    # address_transliterator = address_translator.transliterate_address
    # transliterate_address = udf(lambda x: address_transliterator(x), StringType())

    # get_names_mapped = NamesTranslator(["marathi"]).translate_name
    # names_mapper = udf(lambda x: get_names_mapped(x), ArrayType(StringType()))

    # parse_raw_village = udf(lambda text : html_parser.parse_village_from_html_text(text), StringType())
    # raw_village_mapper = EntityMapper(mapper_type="village", languages=["marathi"]).map_entity
    # village_mapper = udf(lambda text: raw_village_mapper(text), StringType())

    # convert_year_to_int = udf(lambda text : html_parser.convert_year_to_int(text), IntegerType())

    # reclean_status_updater = udf(lambda value : general.update_reclean_status(value), IntegerType())

    # parse_address_to_components = AddressParser(languages=["english"]).parse_address_to_components
    # address_parser = udf(lambda x: parse_address_to_components(x), StringType())

    print('Done!')
    pipeline_defintion = {
        # 1 : {
        #     'function' : get_deeds_info_from_html_text, # done
        #     'source_column' : 'index_html',
        #     'destination_column' : 'html_text',
        # },
        # 2 : {
        #     'function' : parse_loan_amount, # done
        #     'source_column' : 'html_text',
        #     'destination_column' : 'loan_amount', 
        # },
        # 3 : {
        #     'function' : parse_prop_desc, # done
        #     'source_column' : 'html_text',
        #     'destination_column' : 'raw_property_details',
        # },
        # 4 : {
        #     'function' : parse_area, 
        #     'source_column' : 'html_text',
        #     'destination_column' : 'raw_area',
        # },
        # 5 : {
        #     'function' : parse_first_party, # done
        #     'source_column' : 'html_text',
        #     'destination_column' : 'raw_first_party_details',
        # },
        # 6 : {
        #     'function' : parse_second_party, # done
        #     'source_column' : 'html_text',
        #     'destination_column' : 'raw_second_party_details',
        # },
        # 7 : {
        #     'function' : parse_mortgage_date, # done
        #     'source_column' : 'html_text',
        #     'destination_column' : 'date_of_mortgage',
        # },
        # 8 : {
        #     'function' : parse_reg_date, # done
        #     'source_column' : 'html_text',
        #     'destination_column' : 'date_of_registration',
        # },
        # 9 : {
        #     'function' : parse_stamp_duty, # done
        #     'source_column' : 'html_text',
        #     'destination_column' : 'stamp_duty',
        # },
        # 10 : {
        #     'function' : parse_date_of_filing, # done
        #     'source_column' : 'html_text',
        #     'destination_column' : 'date_of_filing',
        # },
        # 11 : {
        #     'function' : parse_market_price, # done
        #     'source_column' : 'html_text',
        #     'destination_column' : 'market_price',
        # },
        # 12 : {
        #     'function' : parse_reg_fee, # done
        #     'source_column' : 'html_text',
        #     'destination_column' : 'registration_fee',
        # },
        # 13 : {
        #     'function' : parse_date_of_execution, # done
        #     'source_column' : 'html_text',
        #     'destination_column' : 'date_of_execution',
        # },
        # 14 : {
        #     'function' : parse_date_of_submission,
        #     'source_column' : 'html_text',
        #     'destination_column' : 'date_of_submission',
        # },
        # 15 : {
        #     'function' : parse_filing_amount,
        #     'source_column' : 'html_text',
        #     'destination_column' : 'filing_amount',
        # },
        # 16 : {
        #     'function' : parse_compensation,
        #     'source_column' : 'html_text',
        #     'destination_column' : 'compensation',
        # },
        # 17 : {
        #     'function' : address_mapper,
        #     'source_column' : 'raw_property_details',
        #     'destination_column' : 'address_mapped'
        # },
        # 18 : {
        #     'function' : parse_first_names,
        #     'source_column' : 'raw_first_party_details',
        #     'destination_column' : 'raw_first_party_names'
        # },
        # 19 : {
        #     'function' : parse_second_names,
        #     'source_column' : 'raw_second_party_details',
        #     'destination_column' : 'raw_second_party_names'
        # },
        # 20 : {
        #     'function' : names_mapper,
        #     'source_column' : 'raw_first_party_names',
        #     'destination_column' : 'first_party_names'
        # },
        # 21 : {
        #     'function' : names_mapper,
        #     'source_column' : 'raw_second_party_names',
        #     'destination_column' : 'second_party_names'
        # },
        # 22 : {
        #     'function' : transliterate_address,
        #     'source_column' : 'address_mapped',
        #     'destination_column' : 'address'  
        # },
        # 23 : {
        #     'function' : parse_first_pans,
        #     'source_column' : 'raw_first_party_details',
        #     'destination_column' : 'first_party_pan'  
        # },
        # 24 : {
        #     'function' : parse_second_pans,
        #     'source_column' : 'raw_second_party_details',
        #     'destination_column' : 'second_party_pan'  
        # },
        # 25 : {
        #     'function' : parse_second_pans,
        #     'source_column' : 'raw_second_party_details',
        #     'destination_column' : 'second_party_pan'   
        # },
        # 26 : {
        #     'function' : parse_second_pans,
        #     'source_column' : 'raw_second_party_details',
        #     'destination_column' : 'second_party_pan'
        # },
        # 27  : {
        #     'function' : district_mapper ,
        #     'source_column' : 'raw_district',
        #     'destination_column' : 'district'
        # },
        28  : {
            'function' : deed_type_mapper ,
            'source_column' : 'doc_name',
            'destination_column' : 'deed_type'
        },
        # 29  : {
        #     'function' : sro_mapper ,
        #     'source_column' : 'raw_sro',
        #     'destination_column' : 'sro'
        # },
        # 30 : {
        #     'function' : parse_raw_village ,
        #     'source_column' : 'index_html',
        #     'destination_column' : 'raw_village'
        # },
        # 31 : { 
        #     'function' : village_mapper ,
        #     'source_column' : 'raw_village',
        #     'destination_column' : 'village'
        # },
        # 32 : {
        #     'function' : convert_year_to_int ,
        #     'source_column' : 'raw_year',
        #     'destination_column' : 'year'
        # },
        # 33 : {
        #     'function' : reclean_status_updater ,
        #     'source_column' : 'reclean_status',
        #     'destination_column' : 'reclean_status'   
        # },
        # 34 : {
        #     'function' : address_parser ,
        #     'source_column' : 'address',
        #     'destination_column' : 'address_components'   

        # }
    }

    return pipeline_defintion