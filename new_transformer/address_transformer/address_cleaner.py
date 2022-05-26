import re
import sys

import pandas as pd
import os

class AddressCleaner:
    """
        todo:
            4. refactor
    """

    KNOWLEDGE_BASE_DIR = os.getcwd() + "/new_transformer/knowledge_base/"
    LANG_MAPPER = {
        "marathi": "mar",
        "hindi": "hin",
        "kannada": "kan",
    }

    def _load_mapping_file(
        languages: list, file_directory: str, file_name: str,
    ):

        mapper = {}
        for lang in languages:
            file_location = file_directory + lang + "/" + file_name
            try:
                df = pd.read_csv(file_location)
                df.mapping.fillna(" ", inplace=True)
                _mapper = dict(zip(df.og_representation, df.mapping))
                mapper = {
                    **mapper,
                    **_mapper,
                }
            except FileNotFoundError as e:
                print(e)

        return mapper

    def __init__(self, languages: list = ["english"]) -> None:
        """
            Load keyphrases
        """
        self.noise_phrases = AddressCleaner._load_mapping_file(
            languages,
            file_directory=AddressCleaner.KNOWLEDGE_BASE_DIR,
            file_name="address_noise_phrases.csv",
        )

        # language_code = AddressCleaner.LANG_MAPPER[language]

    def remove_noise(address: str, phrases: list = [], **kwargs):
        """
            Desc:
                FYI - address is in lower case
                Removes exact matching text from address
            todo:
                load phrase list dynamically
        """

        for noise_phrase in phrases:
            address = address = address.replace(
                noise_phrase.lower(), phrases[noise_phrase]
            )

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
        address = AddressCleaner.remove_noise(address, phrases=kwargs["noise_phrases"])
        address = re.sub(r"\s{2,}", " ", address)

        return address

    def normalize_address(address: str):
        address = re.sub(r"\W{4,}", " ", address)
        address = re.sub(r";", ", ", address)
        address = re.sub(r",", ", ", address)
        address = re.sub(r"( )?-( )?", "-", address)
        address = re.sub(r"( )?:( )?", ": ", address)
        address = re.sub(r"^\W+", " ", address)
        address = re.sub(r"\W+$", " ", address)
        address = re.sub(r"\s{2,}", " ", address)
        address = re.sub(r"^\W+", " ", address)
        address = re.sub(r", ,", ", ", address)
        address = re.sub(r"\s{2,}", " ", address)

        return address.strip().title()

    def remove_duplicates(address):

        address = re.sub(r"\d\)( | )?corporation", "TERRALYTICS", address).strip()
        address = address.split("TERRALYTICS")
        address = [i.strip() for i in address if i.strip()]

        address = list(set(address))
        address = "|".join(address)

        return address

    def clean_address(self, address: str):

        self.raw_address = address

        address = AddressCleaner.preprocess_address(
            address, noise_phrases=self.noise_phrases
        )

        address = AddressCleaner.remove_duplicates(address)

        address = AddressCleaner.normalize_address(address)

        return address


if __name__ == "__main__":

    address = "(Property Description) 1) Corporation: पुणे म.न.पा. Other details: Building Name:SPRING MEADOWS BLDG E 7TH FLR AMBEGAON BK PUNE, Flat No:704, Road:-, Block Sector:, Landmark: ( Survey Number: 4 ; HISSA NUMBER: 12,13,23,24 ; ) 2) Corporation: पुणे म.न.पा. Other details: Building Name:PADMAPRASAD APARTMENT 2ND FLOOR HINGNE BK PUNE, Flat No:APT NO 6, Road:-, Block Sector:, Landmark: ( Survey Number: 11/2 PART ; C.T.S. Number: 535 PART ; ) 3) Corporation: पुणे म.न.पा. Other details: Building Name:PADMAPRASAD APARTMENT 2ND FLOOR HINGNE BK PUNE, Flat No:APT NO 6, Road:-, Block Sector:, Landmark: ( Survey Number: 11/2 PART ; C.T.S. Number: 535 PART ; ) 4) Corporation: पुणे म.न.पा. Other details: Building Name:OMKAR APARTMENT 3RD FLOOR VADGAON SHERI PUNE, Flat No:15, Road:-, Block Sector:, Landmark: ( Survey Number: 34,34A/7C/2 ; Plot Number: 7 ; ) 1) Corporation: पुणे म.न.पा. Other details: Building Name:SPRING MEADOWS BLDG E 7TH FLR AMBEGAON BK PUNE, Flat No:704, Road:-, Block Sector:, Landmark: ( Survey Number: 4 ; HISSA NUMBER: 12,13,23,24 ; ) 2) Corporation: पुणे म.न.पा. Other details: Building Name:PADMAPRASAD APARTMENT 2ND FLOOR HINGNE BK PUNE, Flat No:APT NO 6, Road:-, Block Sector:, Landmark: ( Survey Number: 11/2 PART ; C.T.S. Number: 535 PART ; ) 3) Corporation: पुणे म.न.पा. Other details: Building Name:PADMAPRASAD APARTMENT 2ND FLOOR HINGNE BK PUNE, Flat No:APT NO 6, Road:-, Block Sector:, Landmark: ( Survey Number: 11/2 PART ; C.T.S. Number: 535 PART ; ) 4) Corporation: पुणे म.न.पा. Other details: Building Name:OMKAR APARTMENT 3RD FLOOR VADGAON SHERI PUNE, Flat No:15, Road:-, Block Sector:, Landmark: ( Survey Number: 34,34A/7C/2 ; Plot Number: 7 ; )"

    cleaner = AddressCleaner(["english"])
    address = cleaner.clean_address(address)
    print(address)
