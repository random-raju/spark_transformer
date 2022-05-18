import requests
import json
import pyspark
import pandas as pd
import numpy as np
from geopy import distance

import psycopg2
import time


class Tagger:
    """
    The following class initialises tagger object to tags data for the following entities:-
    1. Localities
    2. Projects (rera for now)
    3. Banks and Developers Names

    """

    def __init__(self, data, tag_entity, district=None, to_tag_column='address'):

        self.data = data
        self.tag_entity = tag_entity
        self.district = district
        self.to_tag_column = to_tag_column
        self.data_to_tag = ''
        self.solr_url = "http://172.31.22.174:2517/solr/"

    def process_row(self, response):
        df = pd.DataFrame.from_dict([x for x in json.loads(response.text)['response']['docs']])
        df['clean_hash'] = self.data['clean_hash']

        return [x for x in json.loads(response.text)['response']['docs']]

    def process_batch(self, response):
        a = json.loads(response.text)
        data_df = pd.DataFrame.from_dict(a['response']['docs']).drop_duplicates(subset=['id'])
        data_df.rename(columns={'geom_point_prop_tiger': 'geom_point'}, inplace=True)

        data_df['data_dict'] = data_df.apply(lambda x: dict(x), axis=1)

        length = 0
        mapping_dict_list = []
        for i in self.data_to_tag.split('ZZKKCCXX'):
            mapping_dict = {}
            length += len(i) + 8
            tagged_entities = []
            elements_to_remmove = []
            for idx, j in enumerate(a['tags']):
                if j[3] <= length:
                    tagged_entities.extend(j[7])
                    elements_to_remmove.append(j)
                else:
                    break

            for e in elements_to_remmove:
                a['tags'].remove(e)

            mapping_dict[self.to_tag_column] = i

            if not tagged_entities:
                tagged_entities = []

            mapping_dict['id'] = tagged_entities

            mapping_dict_list.append(mapping_dict)

        mapping_dict_df = pd.DataFrame.from_dict(mapping_dict_list)
        f = mapping_dict_df.explode('id')
        f = f.reset_index(drop=True)

        if data_df.empty:
            f['clean_hash'] = f[self.to_tag_column].apply(lambda x: x.split('*****')[-1])
            f['data_dict'] = None
            return f[['clean_hash', 'data_dict']]

        f = pd.merge(f, data_df[['id', 'data_dict']], on='id', how='left')

        findf = f.groupby(self.to_tag_column)['data_dict'].apply(list)
        findf = findf.reset_index()
        findf['clean_hash'] = findf[self.to_tag_column].apply(lambda x: x.split('*****')[-1])

        findf['data_dict'] = findf['data_dict'].apply(lambda x: None if str(x) == '[nan]' else x)

        return findf[['clean_hash', 'data_dict']]

    def tag_locality(self):
        """
        The following function takes in an input i.e address,
        pass it to solr tag handler & gets relevant response.
        Parameters
        ----------
        address_query: input address
        Returns: solr tag handler response object
        -------
        """

        params = {
            "overlaps": "NO_SUB",
            "tagsLimit": "5000",
            "fl": "id,name,geom_point",
            "wt": "json",
            "indent": "on",
            "matchText": "true",
            "fq": f'''district:"{self.district}"'''
        }
        response = requests.post(
            url=self.solr_url + "profile_localities/tag",
            params=params,
            headers={"Content-Type": "text/plain"},
            data=self.data_to_tag.encode("utf-8"),
            auth=("tealindia_solr_reader", "MSQj55%cRbU5kSq")
        )

        if response.status_code == 200:# and json.loads(response.text)['response']['docs']:
            # if self.tag_type == "batch":
            return self.process_batch(response)
        else:
            return None

    def tag_name(self):
        """
        The following function takes in an input i.e party names involved in a transaction,
        pass it to solr tag handler & gets relevant response.

        Parameters
        ----------
        address_query: input address
        Returns: solr tag handler response object
        -------
        """

        params = {
            "overlaps": "NO_SUB",
            "tagsLimit": "5000",
            "fl": "id,name_legal",
            "wt": "json",
            "indent": "on",
            "matchText": "true",
        }
        response = requests.post(
            url=self.solr_url + "profile_entities/tag",
            params=params,
            headers={"Content-Type": "text/plain"},
            data=self.data_to_tag.encode("utf-8"),
            auth=("tealindia_solr_reader", "MSQj55%cRbU5kSq")
        )

        if response.status_code == 200 and json.loads(response.text)['response']['docs']:
            return self.process_batch(response)
        else:
            data_to_return = pd.DataFrame([a.split('*****')[1] for a in self.data_to_tag.split('ZZKKCCXX')])
            data_to_return.rename(columns={0: 'clean_hash'}, inplace=True)
            data_to_return['data_dict'] = [[] for i in range(len(data_to_return))]
            return data_to_return

    def tag_project(self):
        """
        The following function takes in an input i.e address,
        pass it to solr tag handler & gets relevant response.
        Parameters
        ----------
        address_query: input address
        Returns: solr tag handler response object
        -------
        """

        params = {
            "overlaps": "NO_SUB",
            "tagsLimit": "5000",
            "fl": "id,name,geom_point,geom_point_prop_tiger,locality,type",
            "wt": "json",
            "indent": "on",
            "matchText": "true",
            "fq": f'''district:"{self.district}"'''
        }
        response = requests.post(
            url=self.solr_url + "profile_projects/tag",
            params=params,
            headers={"Content-Type": "text/plain"},
            data=self.data_to_tag.encode("utf-8"),
            auth=("tealindia_solr_reader", "MSQj55%cRbU5kSq")
        )

        if response.status_code == 200: #and json.loads(response.text)['response']['docs']:
            return self.process_batch(response)
        else:
            return None

    def calc_distance(self, set1, set2):

        return distance.distance(set1, set2).km

    def validate_tagged_project(self, row):
        if row['tagged_locality_data']:
            localities = [x['id'] for x in row['tagged_locality_data']]
        else:
            return None

        if not row['tagged_project_data']:
            return None

        mean_longitude = np.mean(
            list(map(float, [x['geom_point'].split(',')[0] for x in row['tagged_locality_data'] if 'geom_point' in x])))
        mean_latitude = np.mean(
            list(map(float, [x['geom_point'].split(',')[1] for x in row['tagged_locality_data'] if 'geom_point' in x])))

        final_project_list = []

        for i in row['tagged_project_data']:
            if 'geom_point' in i:
                geo_distance = self.calc_distance([mean_latitude, mean_longitude],
                                                  list(map(float, i['geom_point'].split(',')))[::-1])
            if i['locality'] in localities or geo_distance <= 10:
                final_project_list.append(i)

        if final_project_list:
            return final_project_list
        else:
            return None

    def main(self):
        if isinstance(self.data, dict):
            self.data = pd.DataFrame.from_dict([self.data])

        if isinstance(self.data, pd.DataFrame):
            self.tag_type = "batch"

        else:
            return "Invalid Data"

        if self.tag_entity == 'address' and self.district:
            self.district = self.data[self.district].unique().tolist()[0]
            self.data_to_tag = 'ZZKKCCXX'.join(
                self.data[self.to_tag_column].astype(str) + '*****' + self.data['clean_hash']).replace(':', ' : ')
            tagged_locality_data = self.tag_locality()
            if isinstance(tagged_locality_data, pd.DataFrame):
                tagged_locality_data.rename(columns={'data_dict': 'tagged_locality_data'}, inplace=True)

            tagged_project_data = self.tag_project()
            if isinstance(tagged_project_data, pd.DataFrame):
                tagged_project_data.rename(columns={'data_dict': 'tagged_project_data'}, inplace=True)

            combined_df = pd.merge(tagged_locality_data, tagged_project_data, on='clean_hash', how='left')

            combined_df['valid_project'] = combined_df.apply(lambda x: self.validate_tagged_project(x), axis=1)
            return combined_df

        elif self.tag_entity == 'name':
            self.data_to_tag = 'ZZKKCCXX'.join(
                self.data[self.to_tag_column].astype(str) + '*****' + self.data['clean_hash']).replace(':', ' : ')

            tagged_name = self.tag_name()

            return tagged_name
        else:
            return "invalid input"


if __name__ == '__main__':
    # clean_df_for_write = {"first_party_names":'Credit Agricol Corporate And Investment Bank','clean_hash' : 'asd'}
    # tagger_fp_name = Tagger(clean_df_for_write, 'name', to_tag_column='first_party_names')
    # tagger_fp_name_df = tagger_fp_name.main()
    # print(tagger_fp_name_df.data_dict)

    
    clean_df_for_write = {"address":'Building Name:PARITOSH WING C, Flat No:203, Road:BALEWADI, Landmark: ( Survey Number: 31 ; )','clean_hash' : 'asd'}
    tagger_fp_name = Tagger(clean_df_for_write, 'address', to_tag_column='address')
    tagger_fp_name_df = tagger_fp_name.main()
    print(tagger_fp_name_df.data_dict)
    # tagger_fp_name_df.rename(columns={'data_dict': 'tagged_entity_first_party'}, inplace=True)