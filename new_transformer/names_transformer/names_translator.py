import re
import sys

import pandas as pd

from new_transformer.utils import transliterator
from new_transformer.utils import convert
import os

class NamesTranslator:
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
        self.names_mapper = NamesTranslator._load_mapping_file(
            languages,
            file_directory=NamesTranslator.KNOWLEDGE_BASE_DIR,
            file_name="party_names_tokens.csv",
        )
        self.alphabets_mapper = NamesTranslator._load_mapping_file(
            languages,
            file_directory=NamesTranslator.KNOWLEDGE_BASE_DIR,
            file_name="alphabets.csv",
        )
        self.numbers_mapper = NamesTranslator._load_mapping_file(
            languages,
            file_directory=NamesTranslator.KNOWLEDGE_BASE_DIR,
            file_name="numbers.csv",
        )

        language = [i for i in languages if i != "english"][0]
        language_code = NamesTranslator.LANG_MAPPER[language]
        self.transliterator = transliterator.get_transliterate_object(src=language_code)

    def transliterate_name(self, name: str):

        if not name:
            return None

        name = self.transliterator.transform(name)
        name = name.replace("\x00", " ")

        return name

    def map_native_lang_numbers_to_english(name: str, mapper: dict):

        for native_lang in mapper:
            eng_num = str(mapper[native_lang])
            name = name.replace(native_lang, eng_num)

        return name

    def map_native_lang_numbers_to_english(name: str, mapper: dict):

        for native_lang in mapper:
            eng_num = str(mapper[native_lang])
            name = name.replace(native_lang, eng_num)

        return name

    def map_native_lang_alphabet_to_english(name: str, mapper: dict):

        for letter in mapper:
            eng_mapping = " " + mapper[letter] + " "
            name = name.replace(f" {letter} ", eng_mapping)

        for token in name.split(" "):
            if re.search(r"[0-9]", token):
                for letter in mapper:
                    eng_mapping = mapper[letter]
                    new_token = token.replace(letter, eng_mapping)
                    name = name.replace(token, new_token)

        return name

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

    def preprocess_name(name: str, **kwargs):
        """
            todo:
                load noises
                load key phrase variations
        """

        variations_mapper = kwargs.get("variations_mapper", {})
        if not name:
            name = ""
        name = name.lower()
        name = NamesTranslator.remove_noise(name)
        name = re.sub(r"\s{2,}", " ", name)
        processed_name = NamesTranslator.add_space_before_and_after_special_chars(name)

        processed_name = NamesTranslator.replace_variations_in_keywords_by_standard_keywords(
            processed_name, key_phrases=variations_mapper
        )

        return processed_name

    def remove_space_before_and_after_special_chars(
        name: str, non_ascii_chars: list = []
    ):
        """
            FYI - Not used yet
            Desc:
                Example input -> output : flat no: 1/2 -> flat no : 1 / 2
        """

        special_chars_in_name = re.findall(
            r"\W", name.encode("ascii", errors="ignore").decode("ascii")
        )
        special_chars_in_name = special_chars_in_name + non_ascii_chars

        special_chars_in_name = list(set(special_chars_in_name))
        for char in special_chars_in_name:
            char = char.strip()
            if char:
                name = name.replace(f" {char} ", f"{char}")
            else:
                pass

        return name

    def normalize_name(name: str):

        if not name:
            return None

        name = NamesTranslator.remove_space_before_and_after_special_chars(name)
        name = re.sub(r"\W{4,}", ",", name)
        name = re.sub(r"( )?,( )?", ", ", name)
        name = re.sub(r":", ": ", name).strip()
        name = re.sub(r"\s{2,}", " ", name)
        name = re.sub(r"&( )?nbsp", " ", name)
        name = re.sub(r"nbsp", " ", name)
        name = re.sub(r"\.{2,}", " ", name)
        name = re.sub(r"\s{2,}", " ", name).strip()
        name = re.sub(r"^\W+", " ", name).strip()
        name = re.sub(r"\W+$", " ", name).strip()
        name = re.sub(r"-", " ", name).strip()
        name = re.sub(r"\s{2,}", " ", name).strip()
        name = re.sub(r"^\W+", " ", name).strip()
        name = re.sub(r"\W+$", " ", name).strip()
        name = re.sub(r"\s{2,}", " ", name).strip()

        return name.title().strip()

    def remove_noise(name: str, phrases: list = [], **kwargs):
        """
            Desc:
                FYI - name is in lower case
                Removes exact matching text from name
            todo:
                load phrase list dynamically
        """
        for noise_phrase in phrases:
            name = name = name.replace(noise_phrase.lower(), " ")

        return name

    def add_space_before_and_after_special_chars(name: str, non_ascii_chars: list = []):
        """
            FYI - Not used yet
            Desc:
                Example input -> output : flat no: 1/2 -> flat no : 1 / 2
        """
        special_chars_in_name = re.findall(
            r"\W", name.encode("ascii", errors="ignore").decode("ascii")
        )
        special_chars_in_name = special_chars_in_name + non_ascii_chars

        special_chars_in_name = list(set(special_chars_in_name))
        for char in special_chars_in_name:
            char = char.strip()
            if char:
                name = name.replace(f"{char}", f" {char} ")
            else:
                pass

        return name

    def replace_variations_in_keywords_by_standard_keywords(
        name: str, key_phrases: dict = {}
    ):
        """
        """

        for key_phrase in key_phrases:
            standard_keyword = key_phrases[key_phrase]
            key_phrase = key_phrase.lower()
            name = re.sub(rf" {key_phrase} ", f" {standard_keyword} ", name)

        name = re.sub(r"\s{2,}", " ", name)

        return name

    def map_tokens(name, mapper):

        tokens = name.split(" ")
        translated_name = ""

        for token in tokens:
            if not token:
                continue
            if mapper.get(token):
                translated_name = translated_name + " " + mapper.get(token) + " "
            else:
                translated_name = translated_name + " " + token + " "

        return translated_name

    def map_names(self, name: str):

        self.raw_name = name

        if NamesTranslator.calculate_no_of_indian_language_tokens(name) == 0:
            return None

        name = NamesTranslator.preprocess_name(name)

        name = NamesTranslator.map_tokens(name, self.names_mapper)
        name = NamesTranslator.map_native_lang_numbers_to_english(
            name, self.numbers_mapper
        )
        name = NamesTranslator.map_native_lang_alphabet_to_english(
            name, self.alphabets_mapper
        )

        name = re.sub(r"\s{2,}", " ", name)
        self.name_mapped = name
        name_mapped = NamesTranslator.normalize_name(name_mapped)

        return name_mapped

    def translate_name(self, name: str):

        self.raw_name = name

        if NamesTranslator.calculate_no_of_indian_language_tokens(name) == 0:
            output = {
                "raw_name": self.raw_name,
                "name_mapped": self.raw_name,
                "name_translit": self.raw_name,
                "clean_name": self.raw_name,
            }
            raw_name = NamesTranslator.normalize_name(self.raw_name)

            return raw_name

        name = NamesTranslator.preprocess_name(name)

        name = NamesTranslator.map_tokens(name, self.names_mapper)
        name = NamesTranslator.map_native_lang_numbers_to_english(
            name, self.numbers_mapper
        )

        name = NamesTranslator.map_native_lang_alphabet_to_english(
            name, self.alphabets_mapper
        )

        name = re.sub(r"\s{2,}", " ", name)
        self.name_mapped = name
        name_translit = self.transliterate_name(name)

        name_translit = NamesTranslator.normalize_name(name_translit)
        self.name_translit = name_translit
        self.clean_name = name_translit  # todo

        output = {
            "raw_name": self.raw_name,
            "name_mapped": self.name_mapped,
            "name_translit": self.name_translit,
            "clean_name": self.name_translit,
        }

        names = self.name_translit.split(',')
        names = [name.strip() for name in names if name.strip()]
        names = list(set(names))

        return names


if __name__ == "__main__":
    a = '-कर्ज घेणार - श्रीमती. भारती रामचंद्र पवार,-कर्ज घेणार - श्री. सोमनाथ रामचंद्र पवार,-कर्ज घेणार - सौ. कल्याणी सोमनाथ पवार,-कर्ज घेणार - श्रीमती. भारती रामचंद्र पवार,-कर्ज घेणार - श्री. सोमनाथ रामचंद्र पवार,-कर्ज घेणार - सौ. कल्याणी सोमनाथ पवार'

    print(NamesTranslator(languages=["marathi"]).translate_name(a))
