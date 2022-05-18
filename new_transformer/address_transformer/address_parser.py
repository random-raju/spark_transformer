import re

import pandas as pd
import os
import json

class AddressParser:
    """
        todo:
            4. refactor
    """

    KEY_PHRASES_TO_ADDRESS_COMPONENT_LABEL = {
        "flat no": "flat_no",
        "house no": "house_no",
        "plot no": "plot_no",
        "survey no": "survey_no",
        "office no": "office_no",
        "shop no": "shop_no",
        "tower no": "tower_no",
        "wing no": "wing_no",
        "floor no": "floor_no",
    }

    KNOWLEDGE_BASE_DIR = os.getcwd() + "/new_transformer/knowledge_base/"

    def _load_property_identifiers_variations_mapping(
        languages: list,
        file_directory: str,
        file_name: str = "property_identifiers.csv",
    ):

        property_variations_mapper = {}
        for lang in languages:
            file_location = file_directory + lang + "/" + file_name
            df = pd.read_csv(file_location)
            variations_mapper = dict(zip(df.og_representation, df.mapping))
            property_variations_mapper = {
                **property_variations_mapper,
                **variations_mapper,
            }

        return property_variations_mapper

    def __init__(self, languages: list = ["english"]) -> None:
        """
            Load keyphrases
        """
        self.property_variations_mapper = AddressParser._load_property_identifiers_variations_mapping(
            languages, file_directory=AddressParser.KNOWLEDGE_BASE_DIR
        )

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

    def remove_leading_zeroes(text, sep_=" "):
        """
        Remove leading zeroes from text
        """

        _clean_text = []
        for i in text.split(sep_):
            while i.startswith("0"):
                i = i[1:]
            else:
                _clean_text.append(i)

        return sep_.join(_clean_text)

    def add_space_before_and_after_special_chars(
        address: str, non_ascii_chars: list = []
    ):
        """
            FYI - Not used yet
            Desc:
                Example input -> output : flat no: 1/2 -> flat no : 1 / 2
        """
        special_chars_in_address = re.findall(
            r"\W", address.encode("ascii", errors="ignore").decode("ascii")
        )
        special_chars_in_address = special_chars_in_address + non_ascii_chars

        special_chars_in_address = list(set(special_chars_in_address))
        for char in special_chars_in_address:
            char = char.strip()
            if char:
                address = address.replace(f"{char}", f" {char} ")
            else:
                pass

        return address

    def remove_space_before_and_after_special_chars(
        address: str, non_ascii_chars: list = []
    ):
        """
            FYI - Not used yet
            Desc:
                Example input -> output : flat no: 1/2 -> flat no : 1 / 2
        """
        special_chars_in_address = re.findall(
            r"\W", address.encode("ascii", errors="ignore").decode("ascii")
        )
        special_chars_in_address = special_chars_in_address + non_ascii_chars

        special_chars_in_address = list(set(special_chars_in_address))
        for char in special_chars_in_address:
            char = char.strip()
            if char:
                address = address.replace(f" {char} ", f"{char}")
            else:
                pass

        return address

    def replace_variations_in_keywords_by_standard_keywords(
        address: str, key_phrases: dict = {}
    ):
        """
        """

        for key_phrase in key_phrases:
            standard_keyword = key_phrases[key_phrase]
            key_phrase = key_phrase.lower()
            address = re.sub(rf" {key_phrase} ", f" {standard_keyword} ", address)

        address = re.sub(r"\s{2,}", " ", address)

        return address

    def preprocess_address(address: str, **kwargs):
        """
            todo:
                load noises
                load key phrase variations
        """

        variations_mapper = kwargs.get("variations_mapper", {})

        if not address:
            address = ""
        address = address.lower()
        address = AddressParser.remove_noise(address)
        address = re.sub(r"\s{2,}", " ", address)
        address = re.sub(r"\+", ",", address)
        address = re.sub(r";", ",", address)
        address = re.sub(r"\n", " ", address)
        address = re.sub(r"\r", " ", address)
        address = re.sub(r" to ", ",", address)
        address = re.sub(r" (th)(\W| )?", "th ", address)
        address = re.sub(r" (st)(\W| )?", "st ", address)
        address = re.sub(r" (nd)(\W| )?", "nd ", address)
        address = re.sub(r" (rd)(\W| )?", "rd ", address)
        address = re.sub(r"( |\W)part( |\W)", ",", address)
        address = re.sub(r"( |\W)?(and|&|amp;)( |\W)?", ",", address)
        address = re.sub(r"\s{2,}", " ", address)
        processed_address = AddressParser.add_space_before_and_after_special_chars(
            address
        )

        processed_address = AddressParser.replace_variations_in_keywords_by_standard_keywords(
            processed_address, key_phrases=variations_mapper
        )

        processed_address = AddressParser.remove_space_before_and_after_special_chars(
            processed_address
        )

        return processed_address

    def clean_parsed_property_identifier(
        prop_identifier: str, key_phrase: str, **kwargs
    ):
        """
            todo:
            Analyse and write this script
        """
        if key_phrase in prop_identifier:
            # print(prop_identifier)
            prop_identifier = re.sub(rf"{key_phrase}", " ", prop_identifier).strip()
            prop_identifier = re.sub(r"no", " ", prop_identifier).strip()
            prop_identifier = re.sub(r" , ", ",", prop_identifier)
            prop_identifier = re.sub(r"(new|old|part|%)", ",", prop_identifier)
            # print(prop_identifier)
            prop_identifier = re.sub(r"^\W+", " ", prop_identifier).strip()
            prop_identifier = re.sub(r"\W+$", " ", prop_identifier).strip()
            prop_identifier = re.sub(r"( |\W)\d+ sq(.*)", " ", prop_identifier).strip()
            # prop_identifier = re.sub(
            #     r"\d+( )?(th|rd|st|nd)(.*)", " ", prop_identifier
            # ).strip()
            prop_identifier = (
                re.sub(r"\d+(th|rd|st|nd)(.*)", " ", prop_identifier).strip().upper()
            )
            prop_identifier = (
                re.sub(r"[a-zA-Z]{3,}(.*)", " ", prop_identifier).strip().upper()
            )
            prop_identifier = (
                re.sub(r" \D{3,}(.*)", " ", prop_identifier).strip().upper()
            )
            # print(prop_identifier)
            # print('-----------------------')
            prop_identifier = re.sub(r"G0", "G ", prop_identifier).strip()
            prop_identifier = re.sub(r"B0", "B ", prop_identifier).strip()
            prop_identifier = re.sub(r"D0", "D ", prop_identifier).strip()
            prop_identifier = re.sub(r"LG0", "LG ", prop_identifier).strip()
            prop_identifier = re.sub(r"UG0", "UG ", prop_identifier).strip()
            prop_identifier = re.sub(r"A0", "A ", prop_identifier).strip()
            prop_identifier = re.sub(r"B0", "B ", prop_identifier).strip()
            prop_identifier = re.sub(r"C0", "C ", prop_identifier).strip()
            prop_identifier = re.sub(r"D0", "D ", prop_identifier).strip()
            prop_identifier = re.sub(r"E0", "E ", prop_identifier).strip()
            prop_identifier = re.sub(r"F0", "F ", prop_identifier).strip()
            prop_identifier = re.sub(r"G0", "G ", prop_identifier).strip()
            prop_identifier = re.sub(r"H0", "H ", prop_identifier).strip()
            prop_identifier = re.sub(r"I0", "I ", prop_identifier).strip()
            prop_identifier = re.sub(r"J0", "J ", prop_identifier).strip()
            prop_identifier = re.sub(r"K0", "K ", prop_identifier).strip()
            prop_identifier = re.sub(r"L0", "L ", prop_identifier).strip()
            prop_identifier = re.sub(r"M0", "M ", prop_identifier).strip()
            prop_identifier = re.sub(r"N0", "N ", prop_identifier).strip()
            prop_identifier = re.sub(r"O0", "O ", prop_identifier).strip()
            prop_identifier = re.sub(r"P0", "P ", prop_identifier).strip()
            prop_identifier = re.sub(r"Q0", "Q ", prop_identifier).strip()
            prop_identifier = re.sub(r"R0", "R ", prop_identifier).strip()
            prop_identifier = re.sub(r"S0", "S ", prop_identifier).strip()
            prop_identifier = re.sub(r"T0", "T ", prop_identifier).strip()
            prop_identifier = re.sub(r"U0", "U ", prop_identifier).strip()
            prop_identifier = re.sub(r"V0", "V ", prop_identifier).strip()
            prop_identifier = re.sub(r"W0", "W ", prop_identifier).strip()
            prop_identifier = re.sub(r"X0", "X ", prop_identifier).strip()
            prop_identifier = re.sub(r"Y0", "Y ", prop_identifier).strip()
            prop_identifier = re.sub(r"Z0", "Z ", prop_identifier).strip()
            prop_identifier = re.sub(r"^\W+", " ", prop_identifier).strip()
            prop_identifier = re.sub(r"\W+$", " ", prop_identifier).strip()
            prop_identifier = re.sub(r"\s{2,}", " ", prop_identifier)
            prop_identifier = re.sub(r"^\W+", " ", prop_identifier).strip()
            prop_identifier = re.sub(r"\W+$", " ", prop_identifier).strip()
            prop_identifier = AddressParser.remove_leading_zeroes(prop_identifier)
        else:
            prop_identifier = ""

        return prop_identifier

    def _handle_multiple_identifiers(prop_identifiers, key_phrase, label):

        prop_identifiers = [
            AddressParser.clean_parsed_property_identifier(i, key_phrase)
            for i in prop_identifiers
        ]

        prop_identifiers = [i.strip() for i in prop_identifiers if i.strip()]
        prop_identifiers = list(set(prop_identifiers))
        prop_identifiers = ",".join(prop_identifiers)
        prop_identifiers = prop_identifiers.split(",")
        prop_identifiers = list(set(prop_identifiers))
        prop_identifiers = [i.strip() for i in prop_identifiers if i.strip()]

        # if len(prop_identifiers) == 1:
        #     address_component = {label: prop_identifiers}
        # else:
        address_component = {label: prop_identifiers}

        return address_component

    def parse_property_identifier(
        address: str, key_phrase: str = None, label: str = None
    ):

        if not key_phrase or not label:
            raise Exception("You must pass keyphrase and label")

        # key_phrase_count = address.count(key_phrase)

        if re.search(rf"{key_phrase}", address):
            prop_identifiers = []
            for match in re.finditer(rf"{key_phrase}", address):
                parsed_address = address[match.start() :]
                prop_identifier = re.search(
                    rf"{key_phrase}(.*)", parsed_address
                ).group()
                prop_identifiers.append(prop_identifier)

            address_component = AddressParser._handle_multiple_identifiers(
                prop_identifiers, key_phrase, label
            )
            return address_component
        else:
            return {}

    def restructure_address_components(address_components: dict):

        if address_components:
            restructured_compoents = {"unit": []}
            for key, value in address_components.items():
                if isinstance(value, list) and value:
                    component = {"numbers": value, "type": key}
                    restructured_compoents["unit"].append(component)
                elif isinstance(value, str) and value:
                    component = {"number": value, "type": key}
                    restructured_compoents["unit"].append(component)
        else:
            restructured_compoents = {"unit": None}

        if restructured_compoents == {"unit": []}:
            restructured_compoents = {"unit": None}

        return restructured_compoents

    def parse_address_to_components(self, address: str, **kwargs):

        self.raw_address = address
        self.preproc_address = AddressParser.preprocess_address(
            address, variations_mapper=self.property_variations_mapper
        )

        address_components = {}

        for keyphrase in AddressParser.KEY_PHRASES_TO_ADDRESS_COMPONENT_LABEL:
            label = AddressParser.KEY_PHRASES_TO_ADDRESS_COMPONENT_LABEL[keyphrase]
            parsed_address_component = AddressParser.parse_property_identifier(
                self.preproc_address, keyphrase, label
            )
            address_components = {**address_components, **parsed_address_component}

        address_components = AddressParser.restructure_address_components(
            address_components
        )
        if not address_components:
            return None

        return json.dumps(address_components)

    def parse_flat_no(
        self, address: str, key_phrase="flat no", label="flat_no", **kwargs
    ):

        self.preproc_address = AddressParser.preprocess_address(
            address, variations_mapper=self.property_variations_mapper
        )

        address = self.preproc_address

        if re.search(rf"{key_phrase}", address):
            prop_identifiers = []
            for match in re.finditer(rf"{key_phrase}", address):
                parsed_address = address[match.start() :]
                prop_identifier = re.search(
                    rf"{key_phrase}(.*)", parsed_address
                ).group()
                prop_identifiers.append(prop_identifier)

            address_component = AddressParser._handle_multiple_identifiers(
                prop_identifiers, key_phrase, label
            )
            value = address_component.get(label, "")
            value = ", ".join(value)

            if value.strip() == ",":
                return None
            else:
                return value
        else:
            return None


if __name__ == "__main__":

    # 255719 address = "(Property Description) 1) Corporation: पिंपरी-चिंचवड म.न.पा. Other details: Building Name:GANESH JOYNEST,WING B, Flat No:910, Road:, Block Sector:, Landmark: ( GAT NUMBER: 1193/1,1193/8,1194 "
    address = "Flat No:16, Floor No:GROUND, Building Name:ATUL PLAZA BUILDING, Block Sector:NEAR ALLHABAD BANK MAHADEVNAGAR , Road:MANJARI BUDRUK, City:Manjari Budruk , District:Pune"
    # count = 0
    # FINAL = []

    # address = """
    #     सदनिका नं: बी-5, माळा नं: तळ, इमारतीचे नाव: गोराई 1 जलधारा को ओप हो सौ ली, ब्लॉक नं: बोरीवली पश्चिम मुंबई, रोड : प्लाट नं 94 आर एस सी 5 गोराई 1, इतर माहिती: 30% घसारा बांधकाम वर्ष-1990 बी.एस.सी. असेसमॅंट टेक्स बिल
    #     """
    print("------------------")
    print(address)
    parser = AddressParser(languages=["english"])
    unit = parser.parse_address_to_components(address)
    print(parser.parse_flat_no(address))

    # unit = unit['unit'][0]
    # nos = unit.get('number','')
    # nos = ', '.join(unit.get('numbers',[])) + nos
    # print(nos)
    # import pandas as pd
    # df = pd.read_csv('./pune_guideline_1234_v2.csv')
    # df.address.fillna('',inplace=True)
    # for i,item in df.iterrows():
    #     item = item.to_dict()
    #     prop_desc_from_html = item["address"]
    #     # print(prop_desc_from_html)
    #     unit = parser.parse_address_to_components(prop_desc_from_html)
