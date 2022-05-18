import json

from address_transformer.address_parser import AddressParser
from address_transformer.address_translator import AddressTranslator


class AddressTransformer:
    def __init__(self, languages: list = ["english"]) -> None:

        self.translator = AddressTranslator(languages)
        self.parser = AddressParser(["english"])

    def transform_address(self, address):

        transformed_addresses = self.translator.translate_address(address)
        address_translit = transformed_addresses["address_translit"]
        address_components = self.parser.parse_address_to_components(address_translit)

        return address_components


if __name__ == "__main__":

    address = "सदनिका नं: बी-5, माळा नं: तळ, इमारतीचे नाव: गोराई 1 जलधारा को ओप हो सौ ली, ब्लॉक नं: बोरीवली पश्चिम मुंबई, रोड : प्लाट नं 94 आर एस सी 5 गोराई 1, इतर माहिती: 30% घसारा बांधकाम वर्ष-1990 बी.एस.सी. असेसमॅंट टेक्स बिल"
    address = ", इतर माहिती: सदनिका, १०१,पहिला मजला बिल्डींग बेनस्टॉन बी शेरले राजन रोड बांद्रा प.मुं. ५०"
    address_transformer = AddressTransformer(["marathi"])
    address_transformer.transform_address(address)
