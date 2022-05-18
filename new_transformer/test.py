import pandas as pd
#import urllib3
#import pysolr
#import simplejson
import requests
from requests.auth import HTTPBasicAuth
import re
##LIST COLLECTIONS
HTTPBasicAuth('tealindia_solr_admin', 'H9TniyoF5mU9i^z')
response = requests.get('http://172.31.23.241:2517/solr/admin/collections?action=LIST&wt=json',auth = ('tealindia_solr_admin', 'H9TniyoF5mU9i^z'))
collections_json = response.json()
collections_list = collections_json['collections'] #list of collections
# print(collections_list)
df2 = pd.DataFrame(collections_list, columns =['Collection Name'])
time_indexed_list = []

for i in collections_list:
    # print(i)
    base_url = "http://172.31.23.241:2517/solr/" + i + "/admin/file?wt=json&file=dataimport.properties&contentType=text%2Fplain%3Bcharset%3Dutf-8"
    # print(base_url)
    response3 = requests.get(base_url, auth= ('tealindia_solr_admin', 'H9TniyoF5mU9i^z'))
    # print(response3)
    time_indexed_text = response3.text
    time_indexed_timestamp =  re.findall(r'(?<=.last_index_time=).*', time_indexed_text)[0]
    time_indexed_timestamp = time_indexed_timestamp.replace('\\', '')
    time_indexed_list.append(time_indexed_timestamp)
    # print(time_indexed)
df2['last_indexed_time'] = time_indexed_list
# print(df2)
df2.to_csv('/home/darshan/solr_last_index_time.csv')