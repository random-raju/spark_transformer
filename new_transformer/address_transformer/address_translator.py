import re
import sys
import os
import pandas as pd

from new_transformer.address_transformer.address_cleaner import AddressCleaner
from new_transformer.utils import transliterator


class AddressTranslator:
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
        languages: list,
        file_directory: str,
        file_name: str = "property_identifiers.csv",
    ):

        mapper = {}
        for lang in languages:
            file_location = file_directory + lang + "/" + file_name
            try:
                df = pd.read_csv(file_location)
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
        self.property_variations_mapper = AddressTranslator._load_mapping_file(
            languages,
            file_directory=AddressTranslator.KNOWLEDGE_BASE_DIR,
            file_name="property_identifiers.csv",
        )
        self.district_mapper = AddressTranslator._load_mapping_file(
            languages,
            file_directory=AddressTranslator.KNOWLEDGE_BASE_DIR,
            file_name="district_mappings.csv",
        )
        self.alphabets_mapper = AddressTranslator._load_mapping_file(
            languages,
            file_directory=AddressTranslator.KNOWLEDGE_BASE_DIR,
            file_name="alphabets.csv",
        )
        self.numbers_mapper = AddressTranslator._load_mapping_file(
            languages,
            file_directory=AddressTranslator.KNOWLEDGE_BASE_DIR,
            file_name="numbers.csv",
        )
        self.address_tokens_mapper = AddressTranslator._load_mapping_file(
            languages,
            file_directory=AddressTranslator.KNOWLEDGE_BASE_DIR,
            file_name="address_tokens.csv",
        )
        self.villages_mapper = AddressTranslator._load_mapping_file(
            languages,
            file_directory=AddressTranslator.KNOWLEDGE_BASE_DIR,
            file_name="villages.csv",
        )

        language = [i for i in languages if i != "english"][0]
        language_code = AddressTranslator.LANG_MAPPER[language]
        self.transliterator = transliterator.get_transliterate_object(src=language_code)
        self.address_cleaner = AddressCleaner(["english"])

    def transliterate_address(self, address: str):

        if not address:
            return None

        if AddressTranslator.calculate_no_of_indian_language_tokens(address) == 0:
            return address

        address = self.transliterator.transform(address)
        address = address.replace("\x00", " ")
        address = address.replace("\xa0", " ")

        return address

    def map_native_lang_numbers_to_english(address: str, mapper: dict):

        for native_lang in mapper:
            eng_num = str(mapper[native_lang])
            address = address.replace(native_lang, eng_num)

        return address

    def map_native_lang_numbers_to_english(address: str, mapper: dict):

        for native_lang in mapper:
            eng_num = str(mapper[native_lang])
            address = address.replace(native_lang, eng_num)

        return address

    def map_native_lang_alphabet_to_english(address: str, mapper: dict):

        for letter in mapper:
            eng_mapping = " " + mapper[letter] + " "
            address = address.replace(f" {letter} ", eng_mapping)

        for token in address.split(" "):
            if re.search(r"[0-9]", token):
                for letter in mapper:
                    eng_mapping = mapper[letter]
                    new_token = token.replace(letter, eng_mapping)
                    address = address.replace(token, new_token)

        return address

    def map_property_identifiers_variations(address: str, mapper: dict):

        for variation in mapper:
            address = address.replace(f" {variation} ", f" {mapper[variation]} ")

        return address

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

    def map_tokens(address, mapper):

        tokens = address.split(" ")
        translated_address = ""

        for token in tokens:
            if not token:
                continue
            if mapper.get(token):
                translated_address = translated_address + " " + mapper.get(token) + " "
            else:
                translated_address = translated_address + " " + token + " "

        return translated_address

    def calculate_no_of_indian_language_tokens(text):
        """
            Simple approach to calculate no
            of tokens in native language
        """
        if not text:
            return 0

        if not text.strip():
            return 0

        text = text.replace("\n", " ")
        text = re.sub(r"\d", " ", text).strip()
        text = re.sub(r" \W ", " ", text).strip()
        text = re.sub(r"[\(\)\|\.,:;{}\-\_\\\/#@$%^&*=+~`<>?\[\]]", " ", text).strip()
        text = re.sub(r"'", " ", text).strip()
        text = re.sub(r'"', " ", text).strip()
        text = re.sub(r"\s{2,}", " ", text).strip()

        total_tokens = len(text.split(" "))
        ascii_entity = text.encode("ascii", errors="ignore").decode("ascii")
        ascii_entity = re.sub(r"\s{2,}", " ", ascii_entity).strip()
        token_after_encoding = len(ascii_entity.split(" "))

        if re.search(r"[a-zA-Z]", ascii_entity):  # if only ascii characters are present
            no_of_tokens = total_tokens - token_after_encoding
        else:
            if not ascii_entity.strip():  # when all tokens are in marathi
                no_of_tokens = (
                    sys.maxsize
                )  # default value to indicate all are in marathi

        return no_of_tokens

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
        address = AddressTranslator.remove_noise(address)
        address = re.sub(r"\s{2,}", " ", address)
        processed_address = AddressTranslator.add_space_before_and_after_special_chars(
            address
        )

        processed_address = AddressTranslator.replace_variations_in_keywords_by_standard_keywords(
            processed_address, key_phrases=variations_mapper
        )

        return processed_address

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

    def normalize_address(address: str):

        address = AddressTranslator.remove_space_before_and_after_special_chars(address)
        address = re.sub(r"\W{4,}", ",", address)
        address = re.sub(r"( )?,( )?", ", ", address)
        address = re.sub(r":", ": ", address)
        address = re.sub(r", ,", " ", address)
        address = re.sub(r"\s{2,}", " ", address)

        return address.title().strip()

    def get_address_mapped(self, address: str):

        self.raw_address = address

        address = self.address_cleaner.clean_address(address)

        if AddressTranslator.calculate_no_of_indian_language_tokens(address) == 0:
            return address

        address = AddressTranslator.preprocess_address(address)

        address = AddressTranslator.map_property_identifiers_variations(
            address, self.property_variations_mapper
        )
        address = AddressTranslator.map_tokens(address, self.district_mapper)
        address = AddressTranslator.map_tokens(address, self.address_tokens_mapper)
        address = AddressTranslator.map_tokens(address, self.villages_mapper)
        address = AddressTranslator.map_native_lang_numbers_to_english(
            address, self.numbers_mapper
        )
        address = AddressTranslator.map_native_lang_alphabet_to_english(
            address, self.alphabets_mapper
        )

        address_mapped = re.sub(r"\s{2,}", " ", address)
        self.address_mapped = address_mapped
        address_mapped = AddressTranslator.normalize_address(address_mapped)

        return address_mapped

    def translate_address(self, address: str, type = 'clean_address'):

        self.raw_address = address

        address = self.address_cleaner.clean_address(address)

        if AddressTranslator.calculate_no_of_indian_language_tokens(address) == 0:

            self.raw_address = self.address_cleaner.clean_address(self.raw_address)

            output = {
                "raw_address": self.raw_address,
                "address_mapped": address,
                "address_translit": address,
                "clean_address": address,
            }

            return output[type]

        address = AddressTranslator.preprocess_address(address)

        address = AddressTranslator.map_property_identifiers_variations(
            address, self.property_variations_mapper
        )
        address = AddressTranslator.map_tokens(address, self.district_mapper)
        address = AddressTranslator.map_tokens(address, self.address_tokens_mapper)
        address = AddressTranslator.map_tokens(address, self.villages_mapper)
        address = AddressTranslator.map_native_lang_numbers_to_english(
            address, self.numbers_mapper
        )
        address = AddressTranslator.map_native_lang_alphabet_to_english(
            address, self.alphabets_mapper
        )

        # address = add_comma(address) #todo
        # address = clean_address(address) # todo

        address = re.sub(r"\s{2,}", " ", address)
        self.address_mapped = address
        address_translit = self.transliterate_address(address)
        address_translit = AddressTranslator.normalize_address(address_translit)
        self.address_translit = address_translit
        self.clean_address = address_translit  # todo

        output = {
            "raw_address": self.raw_address,
            "address_mapped": self.address_mapped,
            "address_translit": self.address_translit,
            "clean_address": self.address_translit,
        }

        return output[type]


if __name__ == "__main__":

    # from pg.teal_data_scraped_clean.mh_gom_rd_deeds_mum import address_cleaner

    # from pg.utils import transformer

    # translit_obj = transformer.get_transliterate_object(src="mar")

    address = ", इतर माहिती: गांव व ग्रामपंचायत मौजे शिक्रापूर,येथील आमचे स्वतंत्र खरेदी मालकीहक्काची व ताबेवहिवाटीची बागायत शेतजमीन मिळकत. गट नं 478 क्षेत्र 01 हे 39 आर अधिक पो ख 0 हे 02 आर आकार 4 रु 84 पैसे यापैकी लिहून देतो ती आमचे मालकीहिष्ष्याची व ताबेवहिवाटीची मिळकत खालील प्रमाणे,गट नं 478 पैकी क्षेत्र 00 हे 01 आर आकार 00रु 03 पैसे वरील प्रमाणे लिहून देणार यांचे हिष्ष्याची बागायत मिळकत क्षेत्र 00 हेक्टर 01 आर मिळकत एक हजार चौ फुट मिळकत सदरची मिळकत जाणे येणेचे वहिवाटीचे मालकीहक्कासह व सुखाधिकारासह"

    translator = AddressTranslator(["marathi"])
    address = translator.translate_address(address)
    # clean_address = address_cleaner.clean_address(
    #     address["address_mapped"], translit_obj, 2
    # )

    # print(clean_address)
    print(address)
