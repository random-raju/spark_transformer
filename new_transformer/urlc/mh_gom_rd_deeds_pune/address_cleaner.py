import collections
import csv
import re

import spacy

sp = spacy.load("en_core_web_sm")
all_stopwords = sp.Defaults.stop_words

STANDARD_REPLACEMENTS = {  # todo
    "Hauso": " House ",
    " Sosa ": " Society ",
    "Nan": " ",
    "Apartment ,": " ",
}

STANDARD_REPLACEMENTS = collections.OrderedDict(
    sorted(STANDARD_REPLACEMENTS.items(), reverse=True)
)

COMMON_NOISE_AFTER_OTHER_INFORMATION = [  # add only single token
    "monthly",
    "months",
    "rent",
    "amount",
    "duration",
    "deposit",
    "term",
    "parking",
    "car",
    "discount",
    "fee",
    "fees",
    "stealth",
    "reward",
    "rs",
    "mortgage",
    "mentioned",
    "foot",
    "loan",
    "total",
    "with",
    "percent",
    "part",
    "area",
    "covered",
    "zone",
    "sq",
    "square",
    "metre",
    "meter",
    "area",
    "terrace",
    "balcony",
    "carpet",
    "rera",
    "income",
    "document",
    # "new",
    # "old",
    "ft",
    "feet",
    "construction",
    "level",
    "stilt",
    "department",
    "sadar",
    "built",
    "undvided",
    "undivided",
    "tax",
    "sahit",
    "letter",
    "credit",
    # "resident",
    "flat",
    "ek",
    "annual",
    "term",
    "sub",
    "meter",
    "compensation",
    "charged",
    "agricultural",
    "reconvience",
    "description",
    "will",
    "write",
    "take",
    "read",
    "fit",
    "thats",
    "full",
    "sqaure",
    "including",
    "per",
    "rera",
    "income",
    "Record",
    "Document",
    # "number",
    "Cancelled",
    "pan",
    "year",
    "date",
    "agreement",
    "booked",
    "enclose",
    "other",
    "information",
    "description",
    "sale",
    "hectare",
    "godown",
    "gas",
    "store",
    # "plot", # must analyse
    "khasra",
    "size",
    "mumbai",
    "paiki",
    "theof",
    "of",
    "certificate",
    "Pan",
    "suit",
    "given",
    "open",
    "land",
    "sold",
    "sell",
    "full",
    "gallery",
    "lophat",
    "Extension",
    "with",
    "Contiguous",
    "So",
    "Many",
    "measured",
    "Wrong",
    "Repair",
    "anvay",
    "Lease",
    "Document",
]

REMOVE_BY_KEYWORDS = [  # keywords with some regex , you can mulitple tokens
    "Total",
    "rate",
    "area",
    "term",
    "\d+? month(s)?",
    "month(s)?",
    "\d+? monthly",
    "monthly",
    "Premises",
    "Duration",
    "Deposit",
    "fees",
    # "st",
    "fee",
    "\d+\/-",
    "compensation",
    "market price",
    "price",
    "बाजूची",
    "Length",
    "Prati",
    "Metre",
    "Influence",
    "\d+ year(s)?",
    "year(s)?",
    "Size",
    "Penalty",
    "execution",
    "Mentioned",
    "Pramane",
    "Place Agreement",
    "विनामुल्य देणार",
    "यी जागेचा क",
    "दस्तात नमूद केल्",
    "मिळकतीवर",
    "प्रमाणे",
    "Full Mumbai",
    "R C C",
    "Document No",
    "जीचा Registration",
    "दस्त",
    "list No",
    "Thats चुकीचे",
    "Thats Read",
    "दस्TheाThe",
    "असं वाचण्",
    "The Contract",
    "Contract",
    "Carpet",
    "built up",
    "Mortgage",
    "ळकThe",
    "\d+ (Percent|%)",
    "Advance",
    "rent",
    "Rcc",
    "R c c",
    "car",
    "parking",
    # 'income',
    "Portion",
    "Write",
    "take",
    "next",
    "loan" "amount",
    "construction",
    "Presented Original",
    "\d+\.\d+ sq",
    "Builtup",
    "लायसन्स ",
]

COMMON_NOISE_AFTER_OTHER_INFORMATION = list(set(COMMON_NOISE_AFTER_OTHER_INFORMATION))

COMMON_NOISE_AFTER_OTHER_INFORMATION = [
    i.lower() for i in COMMON_NOISE_AFTER_OTHER_INFORMATION
]


def remove_dates(text):
    """
        Remove dates using regex based on pattern observed in the data
    """
    text = re.sub(
        r"(date|dateनांक)(\W)?\d{1,2}(\.|\/|-| )\d{1,2}(\.|\/|-| )\d{1,4}", " ", text
    )
    text = re.sub(r"\s{2,}", " ", text)
    text = re.sub(r"date(\W)?(\W)?(\W)?\d+ \D+ \d+", " ", text)
    text = re.sub(r"\s{2,}", " ", text)

    return text.strip()


def _remove_other_info_by_approximation(text):
    """
        Based on COMMON_NOISE_AFTER_OTHER_INFORMATION tokens
        see the weight of these tokens in original text corpse
        after the other information section of the text
    """

    if re.search(r"other information(.*)", text):
        other_info_segment = re.search(r"other information(.*)", text).group()
    else:
        other_info_segment = text

    other_info_segment = re.sub(r"other information", " ", other_info_segment)
    other_info_segment = re.sub(r"\d", " ", other_info_segment)
    other_info_segment = re.sub(r" ", " ", other_info_segment)
    other_info_segment = re.sub(
        r"[\(\)\|\.,:;{}\-\_\\\/#@$%^&*=+~`<>?\[\]]", " ", other_info_segment
    ).strip()
    other_info_segment = other_info_segment.encode("ascii", errors="ignore").decode(
        "ascii"
    )
    other_info_segment = re.sub(r"\s{2,}", " ", other_info_segment)

    other_info_segment_tokens = other_info_segment.split(" ")
    other_info_segment_tokens = [
        word for word in other_info_segment_tokens if not word in all_stopwords
    ]

    other_info_segment_tokens_dict = collections.OrderedDict()

    for i in other_info_segment_tokens:
        i = i.lower()
        if i and len(i) > 1:
            if (
                i in other_info_segment_tokens_dict
                and i in COMMON_NOISE_AFTER_OTHER_INFORMATION
            ):
                other_info_segment_tokens_dict[i] += 1
            else:
                if i in COMMON_NOISE_AFTER_OTHER_INFORMATION:
                    other_info_segment_tokens_dict[i] = 1
                else:
                    other_info_segment_tokens_dict[i] = 0

    total_tokens = len(other_info_segment_tokens_dict)

    total_noise_tokens = len(
        {k: v for k, v in other_info_segment_tokens_dict.items() if v >= 1}
    )
    difference = total_tokens - total_noise_tokens

    return (difference, other_info_segment_tokens_dict)


def remove_other_information(text):
    """
        Remove other information section:
            1. First by approximate comparison
            2. Check the occurance of `other information` and choose correct address section
    """
    text = re.sub(r"\s{2,}", " ", text)

    if re.search(r"other information(.*)", text):
        difference, _ = _remove_other_info_by_approximation(text)
        if difference <= 4:
            text = re.sub(r"other information(.*)", " ", text)
        if text.count("other information") > 1:
            text = re.sub(r"other information(.)?(:)?", "other information", text)
            text = re.sub(r"road no:", "road:", text)
            text_split = text.split("other information")

            if len(text_split) == 3:
                first_address = text_split[0].strip()
                first_address = re.sub(r"\W", " ", first_address)
                first_address = re.sub(r"\s{2,}", " ", first_address).strip()
                second_address = text_split[1].strip()
                second_address = re.sub(r"\W", " ", second_address)
                second_address = re.sub(r"\s{2,}", " ", second_address).strip()
                if first_address == second_address:
                    text = text_split[1]
                # check for length and assign and check tokens
                else:
                    if len(text_split[1]) > len(text_split[0]):
                        text = text_split[1]
                    else:
                        text = text_split[0]

    _, tokens_analysis_dict = _remove_other_info_by_approximation(text)
    text = remove_noise_based_on_tokens_analysis(text, tokens_analysis_dict)

    return text.strip()


def normalize_text(text):

    text = text.replace(" . ", ".")
    text = text.replace(" , ", ", ")
    text = text.replace(" / ", "/")
    text = text.replace(" : ", ":")
    text = text.replace(" ) ", ")")
    text = text.replace(" ( ", "(")
    text = text.replace(" - ", "-")
    text = text.replace(" [ ", "[")
    text = text.replace(" ] ", "]")
    text = text.replace(" \ ", " \\ ")
    text = text.replace(" ; ", " ; ")
    text = text.replace(" + ", "+")
    text = text.replace(" ~ ", "~")
    text = text.replace(" – ", "–")
    text = text.replace(' " ', '"')

    text = text.replace("plot.khasra", "khasra")
    text = text.replace("sadar income survey", "Income")
    text = text.replace("metre", "meter,")
    text = text.replace("\ u200d", " ")
    text = text.replace("sector", ",sector")

    text = re.sub(r"\s{2,}", " ", text)

    return text.lower().strip()


def pre_proc_text(text):

    text = text.replace(".", " . ")
    text = text.replace(",", " , ")
    text = text.replace("/", " / ")
    text = text.replace(":", " : ")
    text = text.replace(")", " ) ")
    text = text.replace("(", " ( ")
    text = text.replace("-", " - ")
    text = text.replace("[", " [ ")
    text = text.replace("]", " ] ")
    text = text.replace("\\", " \\ ")
    text = text.replace(";", " ; ")
    text = text.replace("+", " + ")
    text = text.replace("~", " ~ ")
    text = text.replace("%", " % ")
    text = text.replace("&", " & ")
    text = text.replace("#x0D", " ")
    text = text.replace("–", " – ")
    text = text.replace('"', ' " ')
    text = text.replace("'", " ' ")
    text = text.replace("*", " ")
    text = text.replace("”", " ")

    text = " " + text + " "

    return text


def remove_noise_based_on_tokens_analysis(text, tokens_analysis_dict):

    tokens = list(tokens_analysis_dict.keys())
    noise_index_list = []

    for i in range(len(tokens)):
        max_noise_threshold = 4
        while i < (len(tokens) - 2):
            token = tokens[i]
            if tokens_analysis_dict[token] != 0:
                i_plus_one_token = tokens[i + 1]
                i_plus_two_token = tokens[i + 2]
                if (
                    tokens_analysis_dict[i_plus_one_token]
                    and tokens_analysis_dict[i_plus_two_token]
                ):
                    noise_index_list.append(token)
                i += 1
                max_noise_threshold -= 1

            if max_noise_threshold == 0:
                break
            else:
                break

        if i > len(tokens) or max_noise_threshold == 0:
            break

    if noise_index_list:
        noise = noise_index_list[0].lower()
        text = pre_proc_text(text)
        text = re.sub(rf" {noise} (.*)", "", text)
        text = normalize_text(text)

    text = re.sub(rf"other information(:)?(,)?", "", text).strip()
    text = re.sub(rf":,", "", text).strip()

    return text


def remove_leading_zeroes(address, sep_=" "):
    """
    """
    _clean_address = []
    for i in address.split(sep_):
        while i.startswith("0"):
            i = i[1:]
        else:
            _clean_address.append(i)

    return sep_.join(_clean_address)


def remove_by_keyword(keyword, text):
    """
        keep removing util keyword is not in address uses `,` as delimiter
        todo:
            validate
            get most common delimiter
            keep adding ,
    """
    keyword = keyword.lower()
    for i in re.finditer(keyword, text):
        start = i.start()
        end = text.find(",", i.start())
        if end == -1:
            break
        text = text[:start] + "," + text[end + 1 :]
        text = remove_by_keyword(keyword, text)
        break

    return text


def final_processing(text):
    """
        Translit only when number of tokens is greater than 0
    """
    if not text:
        return None

    text = re.sub(r"\s{2,}", " ", text)
    # text = re.sub(r'\W+',' , ',text).strip()
    text = re.sub(r"(,|-){2,}", " ", text)
    text = re.sub(r"(, ){2,}", " ", text)
    text = re.sub(r"(- ){2,}", " ", text)
    text = re.sub(r"\)", " ) ", text)
    text = re.sub(r"\(", " ( ", text)

    text = re.sub(r"^\W+", " ", text).strip()
    text = re.sub(r"\W+$", " ", text).strip()
    text = re.sub(r"\.", " ", text)
    text = re.sub(r"\s{2,}", " ", text).strip()
    text = text.replace("\x00", " ").strip()
    text = text.replace("\\x00", " ").strip()

    if not text:
        return None

    text = remove_leading_zeroes(text)

    return text.title().strip()


def clean_address(n_p_n_t_mapped):
    """
        Handle cleaning of address by classifying them
        todo :
            1. Remove pins
            3. Take care of special characters
            4. Add some common mappings
            5. Remove common mappings mistakes
            6. Add Tokens to STANDARD_REPLACEMENTS
            7. Map letters
            8. Set empty addresses to null
    """

    if not n_p_n_t_mapped:
        return None

    n_p_n_t_mapped = n_p_n_t_mapped.lower().strip()
    n_p_n_t_mapped = " ,, " + ",,,," + n_p_n_t_mapped + ","
    n_p_n_t_mapped = normalize_text(n_p_n_t_mapped)
    n_p_n_t_mapped = n_p_n_t_mapped.replace("/-", "/-,")

    for keyword in REMOVE_BY_KEYWORDS:
        n_p_n_t_mapped = remove_by_keyword(keyword, n_p_n_t_mapped)

    n_p_n_t_mapped = remove_leading_zeroes(n_p_n_t_mapped)

    address = ""

    if re.search(r"^apartment no", n_p_n_t_mapped) or re.search(
        r"^apartment / flat no", n_p_n_t_mapped
    ):  # when text starts with apartment no

        address = normalize_text(n_p_n_t_mapped)
        address = remove_dates(address)
        address = remove_other_information(address)
        address = address.title()

    else:
        address = normalize_text(n_p_n_t_mapped)
        address = remove_dates(address)
        _, tokens_analysis_dict = _remove_other_info_by_approximation(address)
        address = remove_noise_based_on_tokens_analysis(address, tokens_analysis_dict)
        address = address.title()

    return final_processing(address)


if __name__ == "__main__":
    a = """
    Mauje Avale, Taluka Bhiwandi, District Thane, Survey No 81/1/2/And/3/3/A/1 Non-Agricultural Jaage, ' Sai Charan ', Nakhandne Surveyboxan, The Aalel, R C C Survey\x00Andrupac, Imaranthhe, Second Floor Apartment No 202 Of Apartment Ownership Hakk Thethe\x00And\x00Andra(Onership Bemshurveyshurveyne ), J, Survey Gram Panchayat House No 73(2)2 Avale Athshurvey Athshurveyn Chatai
    """
    print(a)
    print()
    "Other Information: , Other Information: पर्,यी जागेचा करार भाडेकरूंना Old जागेच्, बदल्,त विनामुल्य देणारी New जागा,Old Room No. 14,Area 218 Sq. Ft. कार्पेट,1 st Floor,हाजी बापू मंजिल,दत्त Mandir Road,Malad East,Mumbai 400097. त्,च्, बदल्,त दस्तात नमूद केल्,प्रमाणे Sadar मिळकतीवर प्रायोजित New इमारतीत देण्,चे New Area 218 Sq. Ft. किंवा Area 254 Sq. Ft.,Sadar Income C.T.S. No. 232,232 1 ते 8,Mauje Malad East , is."

    """
    Other Information : Mauje Dongare ( Naringi ) , Division 5 , Survey No . 45 , Part No . 1 ते 7 , Survey No . 46 , Part No . 1 , Survey No . 47 , Part No . 1 ते 5 , Survey No . 48 , Part No . 2 , 6 ते 11 , Survey No . 49 , Part No . 1 , Survey No . 66 , Part No . 13 , 14 , 15 , 16 / 4 , 16 / 5 , Survey No . 146 , Part No . 6 , 8 And 9 ते 11 , Survey No . 91 , Part No . 1 - 2 , 2 , Flat No . 307 , wing D , Third Floor , Area 37 . 62 Sq . M . Carpet , Vinay Unique इंपिरी , Building No . 23 , type Q8a , Sector - 7
    """

    """
    Other Information : , Other Information : पर् , यी Place Agreement भाडेकरूंना Old जागेC , Badal , The विनामुल्य देणारी New Area , Old Room No . 14 , Area 218 Sq . Ft . Carpet , 1 st Floor , Haji Bapu Floor, , दThe्The Mandir Road , Malad East , Mumbai 400097 . The् , C , Badal , The दस्TheाThe Mentioned If , Pramane Sadar मिळकTheीवर प्रायोजिThe New इमारTheीThe देण् , Of New Area 218 Sq . Ft . Or Area 254 Sq . Ft . , Sadar Income C . T . S . No . 232 , 232 1 Theे 8 , Mauje Malad East , is .
    """
    # need to pass translit obj and no of tokens
    print(clean_address(a))