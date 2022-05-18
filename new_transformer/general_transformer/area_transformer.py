import re
import os
import pandas as pd

from new_transformer.address_transformer import address_translator


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
        text = re.sub(r"(mter|feet|foot|meter|metre)\.?", " meter ", text)
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

        for type in AreaConverter.AREA_UNITS:
            if type in text:
                return type

        return None

    def convert_to_square_feet(self, text):

        area_dict = {"open_area_sqft": [], "build_area_sqft": [], "carpet_area": []}

        translations = self.translator.translate_address(text)
        clean_text = translations["clean_address"]
        clean_text = AreaConverter.replace_variations_by_constants(text)

        clean_text = clean_text.split("|")
        clean_text = [i.strip() for i in clean_text if i.strip()]

        for area in clean_text:
            area_type = AreaConverter.get_area_type(area)
            area_unit = AreaConverter.get_area_unit(area)
            if not area_unit:
                continue
            if area_unit and not area_type:
                std_area_type = "open_area_sqft"
            else:
                std_area_type = AreaConverter.AREA_TYPES[area_type]

            conversion_factory = AreaConverter.AREA_UNITS[area_unit]
            area = AreaConverter.clean_area(area)
            area = round(float(area), 2)
            area_sqft = area * conversion_factory
            area_dict[std_area_type].append(area_sqft)
            area_dict[std_area_type] = list(set(area_dict[std_area_type]))

        return area_dict


if __name__ == "__main__":

    area = """
    1) Build Area :0.00 / Open Area :11000 Square Meter 2) Build Area :0.00 / Open Area :12000 Square Meter 3) Build Area :0.00 / Open Area :2300 Square Meter 4) Build Area :0.00 / Open Area :5700 Square Meter 5) Build Area :0.00 / Open Area :6600 Square Meter
    """
    convertor = AreaConverter(languages=["marathi", "english"])
    area = convertor.convert_to_square_feet(area)
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
