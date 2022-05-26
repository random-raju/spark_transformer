from pyspark.sql.functions import udf
from new_transformer.general_transformer.entity_mapper import EntityMapper
from new_transformer.address_transformer.address_translator import AddressTranslator
from pyspark.sql.types import StringType, DateType, IntegerType , FloatType, ArrayType, TimestampType
from new_transformer.urlc.mh_gom_rd_deeds_pune import html_parser
from new_transformer.names_transformer.names_translator import NamesTranslator
from new_transformer.utils import general,convert
from new_transformer.address_transformer.address_parser import AddressParser
from new_transformer.urlc.mh_gom_rd_deeds_pune.address_cleaner import clean_address


def raw_to_clean_pipeline_definitions():

    raw_deed_entity_mapper = EntityMapper(mapper_type="raw_deed", languages=["english"]).map_entity
    deed_type_transformer_mapper = udf(lambda text : raw_deed_entity_mapper(text), StringType())

    deed_entity_mapper = EntityMapper(mapper_type="deed", languages=["english"]).map_entity
    deed_type_teal_mapper = udf(lambda text : deed_entity_mapper(text), StringType())

    raw_district_mapper = EntityMapper(mapper_type="district", languages=["marathi"]).map_entity
    district_mapper = udf(lambda text : raw_district_mapper(text), StringType())

    raw_sro_mapping = EntityMapper(mapper_type="sro", languages=["english"]).map_entity
    sro_mapper = udf(lambda text : raw_sro_mapping(text), StringType())

    get_deeds_info_from_html_text = udf(lambda text : html_parser.get_html_as_str(text), StringType())

    parse_prop_desc = udf(lambda text : html_parser.parse_prop_desc(text), StringType())

    parse_area = udf(lambda text : html_parser.parse_area(text), StringType())

    parse_first_party = udf(lambda text : html_parser.parse_first_party(text), StringType())
    parse_first_names = udf(lambda text : html_parser.parse_party_names(text), StringType())
    parse_first_pans = udf(lambda text : html_parser.parse_party_pan(text), ArrayType(StringType()))

    parse_age = udf(lambda text : html_parser.get_age(text), ArrayType(IntegerType()))
    parse_pincode = udf(lambda text : html_parser.get_pincode(text), ArrayType(StringType()))

    parse_second_party = udf(lambda text : html_parser.parse_second_party(text), StringType())
    parse_second_names = udf(lambda text : html_parser.parse_party_names(text), StringType())
    parse_second_pans = udf(lambda text : html_parser.parse_party_pan(text), ArrayType(StringType()))

    parse_mortgage_date = udf(lambda text : html_parser.parse_mortgage_date(text), TimestampType())
    parse_date_of_filing = udf(lambda text : html_parser.parse_date_of_filing(text), TimestampType())
    parse_date_of_execution = udf(lambda text : html_parser.parse_date_of_execution(text), TimestampType())
    parse_date_of_submission = udf(lambda text : html_parser.parse_date_of_submission(text), TimestampType())

    parse_reg_date = udf(lambda text : convert.to_date(text), TimestampType())

    parse_stamp_duty = udf(lambda text : html_parser.parse_stamp_duty(text), FloatType())
    parse_filing_amount = udf(lambda text : html_parser.parse_filing_amount(text), FloatType())
    parse_compensation = udf(lambda text : html_parser.parse_compensation(text), FloatType())
    parse_market_price = udf(lambda text : html_parser.parse_market_price(text), FloatType())
    parse_reg_fee = udf(lambda text : html_parser.parse_reg_fee(text), FloatType())
    parse_loan_amount = udf(lambda text : html_parser.parse_loan_amount(text), FloatType())
    parse_rent = udf(lambda text : html_parser.parse_rent(text), FloatType())
    parse_deposit = udf(lambda text : html_parser.parse_deposit(text), FloatType())

    translate_address = AddressTranslator(["marathi"]).translate_address
    address_mapper = udf(lambda x: translate_address(x), StringType())

    address_formatter_and_clean = udf(lambda x: clean_address(x), StringType())

    get_names_mapped = NamesTranslator(["marathi"]).translate_name
    names_mapper = udf(lambda x: get_names_mapped(x), ArrayType(StringType()))

    parse_raw_village = udf(lambda text : html_parser.parse_village_from_html_text(text), StringType())
    raw_village_mapper = EntityMapper(mapper_type="village", languages=["marathi"]).map_entity
    village_mapper = udf(lambda text: raw_village_mapper(text), StringType())

    convert_year_to_int = udf(lambda text : html_parser.convert_year_to_int(text), IntegerType())

    address_parser = AddressParser(languages=["english"])

    parse_flat_no = address_parser.parse_flat_no
    flat_no_parser = udf(lambda x: parse_flat_no(x), StringType())

    parse_house_no= address_parser.parse_house_no
    house_no_parser = udf(lambda x: parse_house_no(x), StringType())

    parse_plot_no = address_parser.parse_plot_no
    plot_no_parser = udf(lambda x: parse_plot_no(x), StringType())

    parse_survey_no = address_parser.parse_survey_no
    survey_no_parser = udf(lambda x: parse_survey_no(x), StringType())

    parse_office_no = address_parser.parse_office_no
    office_no_parser = udf(lambda x: parse_office_no(x), StringType())

    parse_shop_no = address_parser.parse_shop_no
    shop_no_parser = udf(lambda x: parse_shop_no(x), StringType())

    parse_tower_no = address_parser.parse_tower_no
    tower_no_parser = udf(lambda x: parse_tower_no(x), StringType())

    parse_wing_no = address_parser.parse_wing_no
    wing_no_parser = udf(lambda x: parse_wing_no(x), StringType())

    parse_floor_no= address_parser.parse_floor_no
    floor_no_parser = udf(lambda x: parse_floor_no(x), StringType())

    get_sale_type = udf(lambda text : html_parser.is_primary_sale(text), StringType())

    get_property_type = udf(lambda text : html_parser.get_property_type(text), StringType())


    pipeline_defintion = {
        1 : {
            'function' : get_deeds_info_from_html_text, # done
            'source_column' : 'index_html',
            'destination_column' : 'html_text',
        },
        2 : {
            'function' : deed_type_transformer_mapper, # done
            'source_column' : 'deed_type_raw',
            'destination_column' : 'deed_type_transform',
        },
        3 : {
            'function' : deed_type_teal_mapper, # done
            'source_column' : 'deed_type_raw',
            'destination_column' : 'deed_type_teal',
        },
        4 : {
            'function' : district_mapper, # done
            'source_column' : 'district_raw',
            'destination_column' : 'district_transform',
        },
        5 : {
            'function' : sro_mapper, # done
            'source_column' : 'sro_raw',
            'destination_column' : 'sro_transform',
        },
        6 : {
            'function' : convert_year_to_int ,
            'source_column' : 'raw_year',
            'destination_column' : 'year'
        },
        7 : {
            'function' : parse_prop_desc ,
            'source_column' : 'html_text',
            'destination_column' : 'property_description_raw'
        },
        8 : {
            'function' : address_mapper ,
            'source_column' : 'html_text',
            'destination_column' : 'property_description_transform'
        },
        9 : {
            'function' : address_formatter_and_clean ,
            'source_column' : 'property_description_transform',
            'destination_column' : 'property_description_transform'
        },
        10 : {
            'function' : parse_stamp_duty, # done
            'source_column' : 'html_text',
            'destination_column' : 'stamp_duty',
        },
        11 : {
            'function' : parse_filing_amount, # done
            'source_column' : 'html_text',
            'destination_column' : 'filing_amount',
        },
        12 : {
            'function' : parse_compensation, # done
            'source_column' : 'html_text',
            'destination_column' : 'consideration_amount',
        },
        13 : {
            'function' : parse_market_price, # done
            'source_column' : 'html_text',
            'destination_column' : 'market_price',
        },
        14 : {
            'function' : parse_reg_fee, # done
            'source_column' : 'html_text',
            'destination_column' : 'registration_fee',
        },
        15 : {
            'function' : parse_loan_amount, # done
            'source_column' : 'html_text',
            'destination_column' : 'loan_amount',
        },
        16 : {
            'function' : parse_deposit, # done
            'source_column' : 'html_text',
            'destination_column' : 'deposit',
        },
        17 : {
            'function' : parse_rent, # done
            'source_column' : 'html_text',
            'destination_column' : 'rent',
        },
        18 : {
            'function' : parse_first_party, # done
            'source_column' : 'html_text',
            'destination_column' : 'first_party_raw',
        },
        19 : {
            'function' : parse_first_names,
            'source_column' : 'first_party_raw',
            'destination_column' : 'raw_first_party_names'
        },
        20 : {
            'function' : names_mapper,
            'source_column' : 'raw_first_party_names',
            'destination_column' : 'first_party_transform'
        },
        21 : {
            'function' : parse_first_pans,
            'source_column' : 'first_party_raw',
            'destination_column' : 'first_party_pan'  
        },
        22 : {
            'function' : parse_age,
            'source_column' : 'first_party_raw',
            'destination_column' : 'first_party_age'  
        },
        23 : {
            'function' : parse_pincode,
            'source_column' : 'first_party_raw',
            'destination_column' : 'first_party_pincode'  
        },
        24 : {
            'function' : parse_second_party, # done
            'source_column' : 'html_text',
            'destination_column' : 'second_party_raw',
        },
        25 : {
            'function' : parse_second_names,
            'source_column' : 'second_party_raw',
            'destination_column' : 'raw_second_party_names'
        },
        26 : {
            'function' : names_mapper,
            'source_column' : 'raw_second_party_names',
            'destination_column' : 'second_party_transform'
        },
        27 : {
            'function' : parse_second_pans,
            'source_column' : 'second_party_raw',
            'destination_column' : 'second_party_pan'  
        },
        28 : {
            'function' : parse_age,
            'source_column' : 'second_party_raw',
            'destination_column' : 'second_party_age'  
        },
        29 : {
            'function' : parse_pincode,
            'source_column' : 'second_party_raw',
            'destination_column' : 'second_party_pincode'  
        },
        30 : {
            'function' : flat_no_parser,
            'source_column' : 'property_description_transform',
            'destination_column' : 'flat_no'  
        },
        31 : {
            'function' : house_no_parser,
            'source_column' : 'property_description_transform',
            'destination_column' : 'house_no'  
        },
        32 : {
            'function' : plot_no_parser,
            'source_column' : 'property_description_transform',
            'destination_column' : 'plot_no'  
        },
        33 : {
            'function' : survey_no_parser,
            'source_column' : 'property_description_transform',
            'destination_column' : 'survey_no'  
        },
        34 : {
            'function' : office_no_parser,
            'source_column' : 'property_description_transform',
            'destination_column' : 'office_no'  
        },
        35 : {
            'function' : shop_no_parser,
            'source_column' : 'property_description_transform',
            'destination_column' : 'shop_no'  
        },
        36 : {
            'function' : tower_no_parser,
            'source_column' : 'property_description_transform',
            'destination_column' : 'tower_no'  
        },
        37 : {
            'function' : wing_no_parser,
            'source_column' : 'property_description_transform',
            'destination_column' : 'wing_no'  
        },
        38 : {
            'function' : floor_no_parser,
            'source_column' : 'property_description_transform',
            'destination_column' : 'floor_no'  
        },
        39 : {
            'function' : floor_no_parser,
            'source_column' : 'property_description_transform',
            'destination_column' : 'floor_no'  
        },
        40 : {
            'function' : parse_area, 
            'source_column' : 'html_text',
            'destination_column' : 'area_raw',
        },
        41 : {
            'function' : parse_area, 
            'source_column' : 'html_text',
            'destination_column' : 'open_area_sqft',
        },
        42 : {
            'function' : parse_area, 
            'source_column' : 'html_text',
            'destination_column' : 'build_area_sqft',
        },
        43 : {
            'function' : parse_area, 
            'source_column' : 'html_text',
            'destination_column' : 'carpet_area_sqft',
        },
        44 : {
            'function' : parse_mortgage_date, # done
            'source_column' : 'html_text',
            'destination_column' : 'mortgage_date',
        },
        45 : {
            'function' : parse_reg_date, # done
            'source_column' : 'reg_date',
            'destination_column' : 'registration_date',
        },
        46 : {
            'function' : parse_date_of_filing, # done
            'source_column' : 'html_text',
            'destination_column' : 'filing_date',
        },
        47 : {
            'function' : parse_date_of_execution, # done
            'source_column' : 'html_text',
            'destination_column' : 'execution_date',
        },
        48 : {
            'function' : parse_date_of_submission, # done
            'source_column' : 'html_text',
            'destination_column' : 'submission_date',
        },
        49 : {
            'function' : parse_raw_village ,
            'source_column' : 'index_html',
            'destination_column' : 'village_raw'
        },
        50 : { 
            'function' : village_mapper ,
            'source_column' : 'village_raw',
            'destination_column' : 'village_transform'
        },
        51 : {
            'function' : get_sale_type ,
            'source_column' : 'first_party_transform',
            'destination_column' : 'sale_type'
        },
        52 : {
            'function' : get_property_type ,
            'source_column' : 'property_description_transform',
            'destination_column' : 'property_type'
        },
    }


    return pipeline_defintion