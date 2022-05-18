import pymongo
import re
import json

# def get_local_mongo_db_connection():

#     mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
#     mongo_db_conn = mongo_client["teal_clean_test"]
#     mongo_collection = mongo_db_conn["deeds"]

#     return mongo_collection


def get_local_mongo_db_connection():

    mongo_client = pymongo.MongoClient("mongodb+srv://teal_data_intern:OLEftqhheufQB7FS@cluster0.t7lcr.mongodb.net/ownership")
    mongo_db_conn = mongo_client["ownership"]
    mongo_collection = mongo_db_conn["pune-temp"]

    return mongo_collection

mongo_collection = get_local_mongo_db_connection()

def get_attribute(raw,transform):

    mongo_attribute = {
            "raw": raw,
			"transform": transform
    }

    return mongo_attribute

def get_dates(row):

    mongo_attribute = {
        'execution': row["date_of_execution"],
        'filing' : row["date_of_filing"],
        'mortgage' : row["date_of_mortgage"],
        'registration' : row["date_of_registration"],
        'submission' : row["date_of_submission"],
    }

    mongo_attribute = { k:v for (k,v) in mongo_attribute.items() if v}  

    return mongo_attribute

def get_party(row, type):

    mongo_attribute  = {
        'raw' : row[f"raw_{type}_party_details"],
        'transform' : row[f"{type}_party_names"],
        'pan' : row[f"{type}_party_pan"],
    }

    return mongo_attribute

def get_value(row):

    mongo_attribute = {
        'consideration' : row["compensation"],
        'loan' : row["loan_amount"],
        'market' : row["market_price"],
    }

    mongo_attribute = { k:v for (k,v) in mongo_attribute.items() if v}  

    return mongo_attribute

def get_fees(row):

    mongo_attribute = {
        'stamp_duty' : row["stamp_duty"],
        'filing' : row["filing_amount"],
        'registration' : row["registration_fee"],
    }

    mongo_attribute = { k:v for (k,v) in mongo_attribute.items() if v}  

    return mongo_attribute

def get_geom_point(text):

    if not text:
        return None

    geom_points = {}

    for match in re.finditer('geom_point=',text):
        id = text[match.end():]
        id = re.sub(r'}(.*)','',id).strip()
        id = re.sub(r',$','',id).strip()
        id = id.split(',')
        id = [float(i) for i in id]
        geom_points = {
            'type' : "Point",
            "coordinates" : id
        }
        break

    return geom_points

def get_locality_details(text):


    localities = []

    names = []
    ids = []
    geom_points = []

    if not text:
        return None

    for match in re.finditer('name=',text):
        name = text[match.end():]
        name = re.sub(r'id=(.*)','',name).strip()
        name = re.sub(r',$','',name).strip()
        names.append(name)

    for match in re.finditer('id=',text):
        id = text[match.end():]
        id = re.sub(r'geom_point=(.*)','',id).strip()
        id = re.sub(r',$','',id).strip()
        ids.append(id)

    for match in re.finditer('geom_point=',text):
        id = text[match.end():]
        id = re.sub(r'}(.*)','',id).strip()
        id = re.sub(r',$','',id).strip()
        id = id.split(',')
        id = [float(i) for i in id]
        geom_points.append(id)
    
    for i in range(len(ids)):
        locality = {}
        try:
            locality['id'] = ids[i]
        except:
            locality['id'] = None
        try:
            locality['name'] = names[i]
        except:
            locality['name'] = None
        try:
            locality['geom_point'] = {
                'type' : "Point",
                'coordinates' : geom_points[i]
            }
        except:
            locality['geom_point'] = None

        localities.append(locality)

    return localities

def get_address_components(row):

    address_components = row['address_components']

    if address_components:
        address_components = json.loads(address_components)
        if address_components == {"unit": None}:
            return None
        else:
            return address_components
    else:
        return None

def re_structure_data_for_mongo(row):
    
    mongo_row = {}

    mongo_row["_id"] = row['_id']
    mongo_row['state'] = get_attribute('Maharashtra','Maharashtra')
    mongo_row['endpoint'] = 'mh_gom_rd_deeds.pune_deeds_regular'
    mongo_row['district'] = get_attribute(row["raw_district"],row['district'])
    mongo_row['village'] = get_attribute(row["raw_village"],row['village'])
    mongo_row['sro'] = get_attribute(row["raw_sro"],row['sro'])
    mongo_row['year'] = get_attribute(row["raw_year"],row['year'])
    mongo_row['document_no'] = row["doc_no"]
    mongo_row['dates'] = get_dates(row)
    mongo_row['transaction_type'] = get_attribute(row["doc_name"],row['deed_type'])
    mongo_row['property_description'] = get_attribute(row["raw_property_details"],row['address_mapped'])
    mongo_row['first_party'] = get_party(row,'first')
    mongo_row['second_party'] = get_party(row,'second')
    mongo_row['address'] = {}
    mongo_row['address']["unit"] = get_address_components(row)
    mongo_row['address']['locality'] = get_locality_details(row['tagged_locality_data'])

    if not mongo_row['address']["unit"]:
        mongo_row['address'].pop("unit")
    if not mongo_row['address']["locality"]:
        mongo_row['address'].pop("locality")

    mongo_row['value'] = get_value(row)
    mongo_row['fees'] = get_fees(row)

    mongo_row["geom_point"]  = get_geom_point(row["tagged_locality_data"])

    return mongo_row



def write_batch(batch):

    # mongo_collection = get_local_mongo_db_connection()
    print(mongo_collection)
    for row in batch:
        row = row.asDict()
        print(row)
        mongo_row = re_structure_data_for_mongo(row)
        print(mongo_row)
        mongo_collection.insert_one(mongo_row)
        print('-------------')

