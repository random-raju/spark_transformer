import pandas as pd

import os

class EntityMapper:
    """
        Used to map entity
    """

    KNOWLEDGE_BASE_DIR = os.getcwd() + "/new_transformer/knowledge_base/"

    MAPPER_TYPE_TO_FILE = {
        "deed": "deed_type_mappings.csv",
        "district": "district_mappings.csv",
        "sro": "sro_mappings.csv",
        "village": "villages.csv",
        "raw_deed": "deed_type_mappings.csv",
    }

    def _load_mapping_file(
        languages: list, file_directory: str, file_name: str, mapper_type=None
    ):

        mapper = {}
        for lang in languages:
            file_location = file_directory + lang + "/" + file_name
            try:
                df = pd.read_csv(file_location)
                if mapper_type != "raw_deed":
                    variations_mapper = dict(zip(df.og_representation, df.mapping))
                else:
                    variations_mapper = dict(zip(df.og_representation, df.doc_name_eng))
                mapper = {
                    **mapper,
                    **variations_mapper,
                }
            except FileNotFoundError as e:
                print(e)

        return mapper

    def __init__(self, mapper_type=None, languages: list = ["english"]) -> None:
        """
            Load mapper
        """
        if not mapper_type or not EntityMapper.MAPPER_TYPE_TO_FILE.get(
            mapper_type, None
        ):
            raise Exception(
                f"Mapper type has to be among {EntityMapper.MAPPER_TYPE_TO_FILE}"
            )

        self.mapper_type = mapper_type
        file_name = EntityMapper.MAPPER_TYPE_TO_FILE[mapper_type]
        self.mapper = EntityMapper._load_mapping_file(
            languages,
            file_directory=EntityMapper.KNOWLEDGE_BASE_DIR,
            file_name=file_name,
            mapper_type=mapper_type,
        )

    def map_entity(self, raw_deed_type):

        if not raw_deed_type or not raw_deed_type.strip():
            return None
        if raw_deed_type.strip() not in self.mapper:
            return None
        else:
            return self.mapper[raw_deed_type.strip()].strip()


if __name__ == "__main__":

    deed_mapper = EntityMapper(mapper_type="deed", languages=["english"])
    print(deed_mapper.map_entity("36-अ-लिव्ह अॅड लायसन्सेस"))

    district_mapper = EntityMapper(mapper_type="district", languages=["marathi"])
    print(district_mapper.map_entity("पुणे"))

    raw_deed_mapper = EntityMapper(mapper_type="village", languages=["marathi"])
    print(raw_deed_mapper.map_entity("&nbsp &nbsp लोणीकंद"))

    raw_deed_mapper = EntityMapper(mapper_type="raw_deed", languages=["english"])
    print(raw_deed_mapper.map_entity("36-अ-लिव्ह अॅड लायसन्सेस"))
