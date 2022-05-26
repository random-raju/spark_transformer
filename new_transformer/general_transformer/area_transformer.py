import re

import pandas as pd

from new_transformer.address_transformer import address_translator
import os

class AreaConverter:
    """
    """

    KNOWLEDGE_BASE_DIR = os.getcwd() + "/new_transformer/knowledge_base/"

    AREA_TYPES = {
        "open": "open_area_sqft",
        "build": "build_area_sqft",
        "carpet_area": "carpet_area_sqft",
    }

    # area unit type and its conversion factor to sqft
    AREA_UNITS = {
        "sq meter": 10.763,
        "sq feet": 1,
        "hectare": 107639,
        "gunta": 101.1714,
        "acre": 43560,
    }

    STD_AREA = {
        "sq(.)?( )?meter": "sq meter",
        "sq(.)?( )?feet": "sq feet",
        "hectare": "hectare",
        "gunta": "gunta",
        "acre": "acre",
    }

    def __init__(self, languages: list = ["english"]) -> None:
        """
            Load keyphrases
        """
        self.translator = address_translator.AddressTranslator(languages)

    def remove_noise(address: str, phrases: list = [], **kwargs):
        """
            Desc:
                FYI - address is in lower case
                Removes exact matching text from address
            todo:
                load phrase list dynamically
        """
        for noise_phrase in phrases:
            address = address = address.replace(noise_phrase.lower(), " ")

        return address

    def replace_variations_by_constants(text):
        """
            Move this to file
        """
        text = text.lower()

        text = re.sub(r"( )?\.( )?", ".", text)
        text = re.sub(r"\d+\)", " ", text)
        text = re.sub(r"open", "|open", text)
        text = re.sub(r"build", "|build", text)
        text = re.sub(r"carpet", "|carpet", text)
        text = re.sub(r"(sqaure|square)\.?", " sq ", text)
        text = re.sub(r"( ft |feet|foot)\.?", "feet ", text)
        text = re.sub(r"(mter|meter|metre)\.?", " meter ", text)
        text = re.sub(r"(guntha|gunthe)\.?", " gunta ", text)
        text = re.sub(r"\s{2,}", " ", text)

        return text

    def clean_area(text):

        text = re.sub(r"[a-zA-Z]+", " ", text).strip()
        text = re.sub(r"^\W+", " ", text).strip()
        text = re.sub(r"\W+$", " ", text).strip()

        return text

    def get_area_type(text):

        for type in AreaConverter.AREA_TYPES:
            if type in text:
                return type

        return None

    def get_area_unit(text):

        for type in AreaConverter.STD_AREA:
            if re.search(rf"{type}", text):
                return AreaConverter.STD_AREA[type]

        return None

    def get_open_area_sqft(dict):

        final_op = []

        for _ in dict:
            if dict["open_area_sqft"]:
                for value in dict["open_area_sqft"]:
                    output = {}
                    output["value"] = value
                    output["unit"] = "SQFT"
                    output["type"] = "Open Area"
                    if not output["value"] or output["value"] == [0]:
                        continue
                    final_op.append(output)

        return final_op

    def restructure_for_mongo(dict):

        final_op = []

        for _ in dict:
            if dict["open_area_sqft"]:
                for value in dict["open_area_sqft"]:
                    output = {}
                    output["value"] = value
                    output["unit"] = "SQFT"
                    output["type"] = "Open Area"
                    if not output["value"] or output["value"] == [0]:
                        continue
                    final_op.append(output)

            if dict["build_area_sqft"]:
                for value in dict["build_area_sqft"]:
                    output = {}
                    output["value"] = value
                    output["unit"] = "SQFT"
                    output["type"] = "Built-up Area"
                    if not output["value"] or output["value"] == [0]:
                        continue
                    final_op.append(output)

            if dict["carpet_area"]:
                for value in dict["carpet_area"]:
                    output = {}
                    output["value"] = value
                    output["unit"] = "SQFT"
                    output["type"] = "Carpet Area"
                    if not output["value"] or output["value"] == [0]:
                        continue
                    final_op.append(output)
            break

        return dict

    def convert_to_square_feet(self, text , type = None):

        area_dict = {"open_area_sqft": [], "build_area_sqft": [], "carpet_area": []}

        translations = self.translator.translate_address(text)
        clean_text = translations
        clean_text = AreaConverter.replace_variations_by_constants(clean_text)

        clean_text = clean_text.split("|")
        clean_text = [i.strip() for i in clean_text if i.strip()]

        for area in clean_text:
            area_type = AreaConverter.get_area_type(area)
            area_unit = AreaConverter.get_area_unit(area)
            if not area_unit:
                area_unit = "sq meter"
            if area_unit and not area_type:
                std_area_type = "open_area_sqft"
            else:
                std_area_type = AreaConverter.AREA_TYPES[area_type]

            conversion_factory = AreaConverter.AREA_UNITS[area_unit]
            area = AreaConverter.clean_area(area)
            if area == "":
                area = 0
            if ":" in area:
                area = 0
            area = round(float(area), 2)
            area_sqft = area * conversion_factory
            area_dict[std_area_type].append(area_sqft)
            area_dict[std_area_type] = list(set(area_dict[std_area_type]))

        for key in area_dict:
            if not area_dict[key] or area_dict[key] == [0]:
                area_dict[key] = None

        # output = AreaConverter.restructure_for_mongo(area_dict)
        if type:
            return area_dict[type]

        return area_dict


if __name__ == "__main__":

    area = """
    1) Build Area :153.00 / Open Area :0 Square Meter 2) Build Area :153.00 / Open Area :0 Square Meter
    """
    convertor = AreaConverter(languages=["marathi", "english"])
    area = convertor.convert_to_square_feet(area,type='build_area_sqft')
    print(area)
# import pandas as pd
# df = pd.read_csv('./pune_area_1.csv')
# df.area.fillna('',inplace=True)
# ROWS = []
# print(area)
# for i,item in df.iterrows():
#     item = item.to_dict()
#     area = item["area"]
#     # print(prop_desc_from_html)
#     area = convertor.convert_to_square_feet(area)
#     area = {
#         "area" : area
#     }
#     ROWS.append(area)
#     print(area)
#     print(i)

# noq = pd.DataFrame.from_dict(ROWS, orient='columns')
# # noq = pd.Series(FINAL)

# noq.to_csv('./z_pune_krat.csv')
