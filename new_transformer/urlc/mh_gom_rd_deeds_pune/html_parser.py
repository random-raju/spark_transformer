import os
from bs4 import BeautifulSoup
import re

from new_transformer.utils import convert
from new_transformer.utils import aws

def get_bs4_html_obj(file_location):
    """
    """
    with open(file_location) as fp:
        html_text = fp.read()
        bs_obj = BeautifulSoup(html_text, "lxml")
        # os.remove(file_location)
        return bs_obj

def get_html_as_str(html_text):

    if not html_text or not html_text.strip():
        return ''

    bs_obj = BeautifulSoup(html_text, "lxml")
    all_html_tables = bs_obj.find_all("table")
    deed_details = all_html_tables[4]
    table_data = deed_details.find_all("td")

    all_values = []
    for data in table_data:
        all_values.append(data.text.strip())

    table_data_as_str = " ".join(all_values)
    table_data_as_str = re.sub(r"\s{2,}", " ", table_data_as_str)

    return table_data_as_str

def parse_village_from_html_text(html_text):

    if not html_text:
        return ""

    bs_obj = BeautifulSoup(html_text, "lxml")
    all_html_tables = bs_obj.find_all("table")

    # if len(all_html_tables) <= 2:
    #     return None
    try:
        prop_details_table = all_html_tables[3]
    except:
        return None
    table_data = prop_details_table.find_all("td")
    text = ""

    for data in table_data:
        text = text + data.text.strip()

    text = re.sub(r'\s{2,}',' ',text)
    village = text.replace("गावाचे", "").lower()
    village = village.replace("नाव", "")
    village = village.replace(":", "")
    village = re.sub(r'\d\)',', ',village)
    village = village.replace("1)", "")
    village = village.replace("&nbsp", "")
    village = village.replace("village name", "")
    village = re.sub(r'\s{2,}',' ',village)
    village = re.sub(r' , ',', ',village)
    village = re.sub(r'^\W,',' ',village)
    village = re.sub(r'\s{2,}',' ',village)
    village = re.sub(r'^,',' ',village)

    village = village.strip().title()

    if not village:
        village = None

    return village

def get_bs4_html_text(file_location):

    with open(file_location) as fp:
        html_text = fp.read()
        bs_obj = BeautifulSoup(html_text, "lxml")
        # os.remove(file_location)
        all_html_tables = bs_obj.find_all("table")
        deed_details = all_html_tables[4]
        table_data = deed_details.find_all("td")

        all_values = []
        for data in table_data:
            all_values.append(data.text.strip())

        table_data_as_str = " ".join(all_values)
        table_data_as_str = re.sub(r"\s{2,}", " ", table_data_as_str)

        return table_data_as_str

def parse_deeds_info_from_html_as_str(bs_obj):
    """
    """

    all_html_tables = bs_obj.find_all("table")
    deed_details = all_html_tables[4]
    table_data = deed_details.find_all("td")

    all_values = []
    for data in table_data:
        all_values.append(data.text.strip())

    table_data_as_str = " ".join(all_values)
    table_data_as_str = re.sub(r"\s{2,}", " ", table_data_as_str)

    return table_data_as_str

def remove_noise_from_amount(text):

    text = str(text)
    text = re.sub(r'\D+',' ',text)
    text = re.sub(r'\/\-',' ',text)
    text = re.sub(r'\-',' ',text).strip()
    text = re.sub(r'^\W+',' ',text).strip()
    text = re.sub(r'\W+$',' ',text).strip()

    return text

def parse_title(deed_details):

    if re.search(
        r"(?<=1\) विलेखाचा प्रकार \(Title\))(.*)(?=\(2\) कर्जाची रक्कम \(Loan amount\))",
        deed_details, #efiling
    ):
        match = re.search(
            r"(?<=1\) विलेखाचा प्रकार \(Title\))(.*)(?=\(2\) कर्जाची रक्कम \(Loan amount\))",
            deed_details,
        ).group()
        return match.strip()
    elif re.search(
        r"(?<=\(1\)विलेखाचा प्रकार)(.*)(?=\(2\)मोबदला)",
        deed_details,
    ): #ereg
        match = re.search(
            r"(?<=\(1\)विलेखाचा प्रकार)(.*)(?=\(2\)मोबदला)",
            deed_details,
        ).group()
        return match.strip()
    else:
        return None

        
def parse_loan_amount(deed_details):

    if re.search(
        r"(?<=\(2\) कर्जाची रक्कम \(Loan amount\))(.*)(?=\(3\) भू-मापन,पोटहिस्सा व)",
        deed_details,
    ):
        match = re.search(
            r"(?<=\(2\) कर्जाची रक्कम \(Loan amount\))(.*)(?=\(3\) भू-मापन,पोटहिस्सा व)",
            deed_details,
        ).group()
    else:
        match = 0.0

    match = remove_noise_from_amount(match)
    amount = convert.to_float(match)

    return amount


def parse_prop_desc(deed_details):

    if re.search(
        r"(?<=\(3\) भू-मापन,पोटहिस्सा व घरक्रमांक\(असल्यास\))(.*)(?=\(4\) क्षेत्रफळ \(Area\))",
        deed_details,
    ): #efiling
        match = re.search(
            r"(?<=\(3\) भू-मापन,पोटहिस्सा व घरक्रमांक\(असल्यास\))(.*)(?=\(4\) क्षेत्रफळ \(Area\))",
            deed_details,
        ).group()
        return match.strip()
    elif re.search(
        r"(?<=\(4\) भू-मापन,पोटहिस्सा व घरक्रमांक\(असल्यास\))(.*)(?=\(5\) क्षेत्रफळ)",
        deed_details,
    ): #ereg
        match = re.search(
            r"(?<=\(4\) भू-मापन,पोटहिस्सा व घरक्रमांक\(असल्यास\))(.*)(?=\(5\) क्षेत्रफळ)",
            deed_details,
        ).group()
        return match.strip()
    else:
        return None

def parse_area(deed_details):

    if re.search(
        r"(?<=\(4\) क्षेत्रफळ \(Area\))(.*)(?=\(5\) कर्ज घेणाऱ्याचे नाव व पत्ता \(Mortgagor\))",
        deed_details,
    ): # efiling
        match = re.search(
            r"(?<=\(4\) क्षेत्रफळ \(Area\))(.*)(?=\(5\) कर्ज घेणाऱ्याचे नाव व पत्ता \(Mortgagor\))",
            deed_details,
        ).group()
        return match.strip()
    elif re.search(
        r"(?<=\(5\) क्षेत्रफळ)(.*)(?=\(6\)आकारणी किंवा जुडी देण्यात असेल तेव्हा)",
        deed_details,
    ):
        match = re.search(
            r"(?<=\(5\) क्षेत्रफळ)(.*)(?=\(6\)आकारणी किंवा जुडी देण्यात असेल तेव्हा)",
            deed_details,
        ).group() #ereg
        return match.strip()
    else:
        return None

def parse_first_party(deed_details):

    if re.search(
        r"(?<=\(5\) कर्ज घेणाऱ्याचे नाव व पत्ता \(Mortgagor\))(.*)(?=\(6\) कर्ज देणाऱ्याचे नाव व पत्ता \(Mortgagee\))",
        deed_details,
    ):
        match = re.search(
            r"(?<=\(5\) कर्ज घेणाऱ्याचे नाव व पत्ता \(Mortgagor\))(.*)(?=\(6\) कर्ज देणाऱ्याचे नाव व पत्ता \(Mortgagee\))",
            deed_details,
        ).group()
        return match.strip()
    elif re.search(
        r"(?<=\(7\) दस्तऐवज करुन देणा-या\/लिहून ठेवणा-या पक्षकाराचे नाव किंवा दिवाणी न्यायालयाचा हुकुमनामा किंवा आदेश असल्यास,प्रतिवादिचे नाव व पत्ता\.)(.*)(?=\(8\)दस्तऐवज करुन घेणा-या पक्षकाराचे व किंवा दिवाणी न्यायालयाचा हुकुमनामा किंवा आदेश असल्यास,प्रतिवादिचे नाव व पत्ता)",
        deed_details,
    ):
        match = re.search(
            r"(?<=\(7\) दस्तऐवज करुन देणा-या\/लिहून ठेवणा-या पक्षकाराचे नाव किंवा दिवाणी न्यायालयाचा हुकुमनामा किंवा आदेश असल्यास,प्रतिवादिचे नाव व पत्ता\.)(.*)(?=\(8\)दस्तऐवज करुन घेणा-या पक्षकाराचे व किंवा दिवाणी न्यायालयाचा हुकुमनामा किंवा आदेश असल्यास,प्रतिवादिचे नाव व पत्ता)",
            deed_details,
        ).group()
        if not match.strip():
            return None
        return match
        # match = re.split(r"\d\)", match)
        # match = list(set(match))
        # return "*)".join(match)

    elif re.search(
        r"(?<=\(7\) Licencsor Name and Address)(.*)(?=\(8\) Licencee Name and Address)",
        deed_details,
        re.I,
    ):
        match = re.search(
            r"(?<=\(7\) Licencsor Name and Address)(.*)(?=\(8\) Licencee Name and Address)",
            deed_details,
        ).group()
        if not match.strip():
            return None
        return match
    else:
        return None

def parse_second_party(deed_details):

    if re.search(
        r"(?<=\(6\) कर्ज देणाऱ्याचे नाव व पत्ता \(Mortgagee\))(.*)(?=\(7\) गहाण \/ कर्जाचा दिनांक \(Date of Mortgage \))",
        deed_details,
    ):
        match = re.search(
            r"(?<=\(6\) कर्ज देणाऱ्याचे नाव व पत्ता \(Mortgagee\))(.*)(?=\(7\) गहाण \/ कर्जाचा दिनांक \(Date of Mortgage \))",
            deed_details,
        ).group()
        return match.strip()
    elif re.search(
        r"(?<=\(8\)दस्तऐवज करुन घेणा-या पक्षकाराचे व किंवा दिवाणी न्यायालयाचा हुकुमनामा किंवा आदेश असल्यास,प्रतिवादिचे नाव व पत्ता)(.*)(?=\(9\) दस्तऐवज करुन दिल्याचा दिनांक)",
        deed_details,
    ):
        match = re.search(
            r"(?<=\(8\)दस्तऐवज करुन घेणा-या पक्षकाराचे व किंवा दिवाणी न्यायालयाचा हुकुमनामा किंवा आदेश असल्यास,प्रतिवादिचे नाव व पत्ता)(.*)(?=\(9\) दस्तऐवज करुन दिल्याचा दिनांक)",
            deed_details,
        ).group()
        if not match.strip():
            return None
        return match.strip()
    elif re.search(
        r"(?<=\(8\) Licencee Name and Address)(.*)(?=\(9\) Date of Execution)",
        deed_details,
    ):
        match = re.search(
            r"(?<=\(8\) Licencee Name and Address)(.*)(?=\(9\) Date of Execution)",
            deed_details,
        ).group()
        if not match.strip():
            return None
        return match.strip()
    else:
        return None

def parse_mortgage_date(deed_details):

    if re.search(
        r"(?<=\(7\) गहाण \/ कर्जाचा दिनांक \(Date of Mortgage \))(.*)(?=\(8\) नोटीस फाईल केल्याचा दिनांक \(Date of filing\))",
        deed_details,
    ):
        match = re.search(
            r"(?<=\(7\) गहाण \/ कर्जाचा दिनांक \(Date of Mortgage \))(.*)(?=\(8\) नोटीस फाईल केल्याचा दिनांक \(Date of filing\))",
            deed_details,
        ).group()
        return convert.to_date(match.strip())
    else:
        return None

def parse_date_of_filing(deed_details):

    if re.search(
        r"(?<=\(8\) नोटीस फाईल केल्याचा दिनांक \(Date of filing\))(.*)(?=\(9\) फायलींग नंबर \(Filing No\.\))",
        deed_details,
    ):
        match = re.search(
            r"(?<=\(8\) नोटीस फाईल केल्याचा दिनांक \(Date of filing\))(.*)(?=\(9\) फायलींग नंबर \(Filing No\.\))",
            deed_details,
        ).group()
        return convert.to_date(match.strip())
    else:
        return None

def parse_date_of_execution(deed_details):

    if re.search(
        r"(?<=\(9\) दस्तऐवज करुन दिल्याचा दिनांक)(.*)(?=\(10\)दस्त नोंदणी केल्याचा दिनांक)",
        deed_details,
    ):
        match = re.search(
            r"(?<=\(9\) दस्तऐवज करुन दिल्याचा दिनांक)(.*)(?=\(10\)दस्त नोंदणी केल्याचा दिनांक)",
            deed_details,
        ).group()
        return convert.to_date(match.strip())
    else:
        return None

def parse_stamp_duty(deed_details):

    if re.search(
        r"(?<=\(10\) मुद्रांक शुल्क \(Stamp Duty\))(.*)(?=\(11\) फायलींग शुल्क \(Filing Amount\))",
        deed_details,
    ):
        match = re.search(
            r"(?<=\(10\) मुद्रांक शुल्क \(Stamp Duty\))(.*)(?=\(11\) फायलींग शुल्क \(Filing Amount\))",
            deed_details,
        ).group()
    elif re.search(
        r"(?<=\(12\)बाजारभावाप्रमाणे मुद्रांक शुल्क)(.*)(?=\(13\)बाजारभावाप्रमाणे नोंदणी शुल्क)",
        deed_details,
    ):
        match = re.search(
            r"(?<=\(12\)बाजारभावाप्रमाणे मुद्रांक शुल्क)(.*)(?=\(13\)बाजारभावाप्रमाणे नोंदणी शुल्क)",
            deed_details,
        ).group()
        
    else:
        match = 0.0

    match = remove_noise_from_amount(match)
    amount = convert.to_float(match)

    return amount

def parse_date_of_submission(deed_details):

    if re.search(
        r"(?<=\(12\) Date of submission)(.*)(?=\(13\) शेरा \(Remark\))", deed_details
    ):
        match = re.search(
            r"(?<=\(12\) Date of submission)(.*)(?=\(13\) शेरा \(Remark\))",
            deed_details,
        ).group()
        return convert.to_date(match.strip())
    else:
        return None

def parse_filing_amount(deed_details):

    if re.search(
        r"(?<=\(11\) फायलींग शुल्क \(Filing Amount\))(.*)(?=\(12\) Date of submission)",
        deed_details,
    ):
        match = re.search(
            r"(?<=\(11\) फायलींग शुल्क \(Filing Amount\))(.*)(?=\(12\) Date of submission)",
            deed_details,
        ).group()
    else:
        match = 0.0

    match = remove_noise_from_amount(match)
    amount = convert.to_float(match)

    return amount

def parse_village(parsed_html_bs_obj):

    if not parsed_html_bs_obj:
        return ""

    all_html_tables = parsed_html_bs_obj.find_all("table")

    if len(all_html_tables) == 4:
        return None

    prop_details_table = all_html_tables[3]
    table_data = prop_details_table.find_all("td")
    text = ""

    for data in table_data:
        text = text + data.text.strip()

    text = re.sub(r'\s{2,}',' ',text)
    village = text.replace("गावाचे", "").lower()
    village = village.replace("नाव", "")
    village = village.replace(":", "")
    village = re.sub(r'\d\)',', ',village)
    village = village.replace("1)", "")
    village = village.replace("&nbsp", "")
    village = village.replace("village name", "")

    village = village.strip().title()

    if not village:
        village = None

    return village

def parse_compensation(text):

    if re.search(r"(?<=\(2\)मोबदला)(.*)(?=\(3\) बाजारभाव)", text):
        match = re.search(r"(?<=\(2\)मोबदला)(.*)(?=\(3\) बाजारभाव)", text).group()
        match = match.strip()
    else:
        match = 0.0

    match = remove_noise_from_amount(match)
    amount = convert.to_float(match)

    return amount

def parse_market_price(text):

    if re.search(
        r"(?<=\(3\) बाजारभाव\(भाडेपटटयाच्या बाबतितपटटाकार आकारणी देतो की पटटेदार ते नमुद करावे\))(.*)(?=\(4\) भू-मापन,पोटहिस्सा व घरक्रमांक\(असल्यास\))",
        text,
    ):
        match = re.search(
            r"(?<=\(3\) बाजारभाव\(भाडेपटटयाच्या बाबतितपटटाकार आकारणी देतो की पटटेदार ते नमुद करावे\))(.*)(?=\(4\) भू-मापन,पोटहिस्सा व घरक्रमांक\(असल्यास\))",
            text,
        ).group()
    else:
        match = 0.0

    match = remove_noise_from_amount(match.strip())
    amount = convert.to_float(match)

    return amount

def parse_reg_fee(text):

    if re.search(
        r"(?<=\(13\)बाजारभावाप्रमाणे नोंदणी शुल्क)(.*)(?=\(14\)शेरा)",
        text,
    ):
        match = re.search(
            r"(?<=\(13\)बाजारभावाप्रमाणे नोंदणी शुल्क)(.*)(?=\(14\)शेरा)",
            text,
        ).group()
    else:
        match = 0.0

    match = remove_noise_from_amount(match)
    amount = convert.to_float(match)

    return amount

def parse_reg_date(deed_details):

    if re.search(
        r"(?<=\(10\)दस्त नोंदणी केल्याचा दिनांक)(.*)(?=\(11\)अनुक्रमांक,खंड व पृष्ठ)", deed_details
    ):
        match = re.search(
            r"(?<=\(10\)दस्त नोंदणी केल्याचा दिनांक)(.*)(?=\(11\)अनुक्रमांक,खंड व पृष्ठ)",
            deed_details,
        ).group()
        return convert.to_date(match.strip())
    else:
        return None

def convert_year_to_int(year):

    # year = convert.to_int(year)

    return year

def parse_party_names(text):

    names = []
    if not text:
        return None

    text = text.lower()
    text = text.replace('\n',' ')
    text = text.replace('\r',' ')

    text = text.replace('इमारतीचे नाव:',"इमारतीचे")

    for match in re.finditer(r'नाव:',text):
        end_index = match.end()
        match = text[end_index:]
        match = re.sub(r'\d\)(.*)', ' ',match).strip()
        match = re.sub(r'वय:(.*)', ' ',match).strip()
        match = re.sub(r'पॅन(.*)', ' ',match).strip()
        match = re.sub(r'पत्ता:(.*)', ' ',match).strip()
        match = re.sub(r'रोड.+:(.*)', ' ',match).strip()
        match = re.sub(r'इमारतीचे(.*)', ' ',match).strip()
        names.append(match)

    text = text.replace('building name',"building")

    for match in re.finditer(r'name:',text):
        end_index = match.end()
        match = text[end_index:]
        match = re.sub(r'age:(.*)', ' ',match).strip()
        match = re.sub(r'block(.*)', ' ',match).strip()
        match = re.sub(r'road(.*)', ' ',match).strip()
        names.append(match)

    names = [name.strip() for name in names if name.strip()]

    if not names:
        return None

    names = ','.join(names)

    return names

def parse_party_pan(text):

    pans = []
    if not text:
        return None

    text = text.lower()
    text = text.replace('\n',' ')
    text = text.replace('\r',' ')
    text = text.replace('\t',' ')
    text = text.replace('pan',"pan:")

    for match in re.finditer(r'पॅन नं',text):
        end_index = match.end()
        match = text[end_index:]
        match = re.sub(r'वय:(.*)', ' ',match).strip()
        match = re.sub(r'\d\)(.*)', ' ',match).strip()
        match = re.sub(r'^\W+', ' ',match).strip()
        match = re.sub(r'^\W+$', ' ',match).strip().upper()
        if match and len(match) == 10:
            pans.append(match)

    for match in re.finditer(r'pan:',text):
        end_index = match.end()
        match = text[end_index:]
        match = re.sub(r'वय:(.*)', ' ',match).strip()
        match = re.sub(r'\d\)(.*)', ' ',match).strip()
        match = re.sub(r'^\W+', ' ',match).strip()
        match = re.sub(r'^\W+$', ' ',match).strip().upper()
        if match and len(match) == 10:
            pans.append(match)

    pans = [pan for pan in pans if pan]

    if not pans:
        return None

    return pans

    # return convert.to_jsonb(pans)


if __name__ == "__main__":

    bucket = 'teal-data-pipeline-mh-gom-rd-deeds-pune'
    file = 'पुणे_Haveli 10 (Bibavewadi)_2019_19835_regular_ereg_combined.html'

    a = ' 1): नाव:-कर्ज घेणार - श्रीमती. भारती रामचंद्र पवार वय:-56; पत्ता:-प्लॉट नं: -, माळा नं: -, इमारतीचे नाव: -, ब्लॉक नं: -, रोड नं: स नं 1001, रामोशी वाडी, वेताळनगर, पी एम सी कॉलनी, गोखलेनगर, पुणे , महाराष्ट्र, पुणे. पिन कोड:-411016 पॅन नं:-CANPP1122M2): नाव:-कर्ज घेणार - श्री. सोमनाथ रामचंद्र पवार वय:-30; पत्ता:-प्लॉट नं: -, माळा नं: -, इमारतीचे नाव: -, ब्लॉक नं: -, रोड नं: स नं 1001, रामोशी वाडी, वेताळनगर, पी एम सी कॉलनी, गोखलेनगर, पुणे, महाराष्ट्र, पुणे. पिन कोड:-411016 पॅन नं:-BPVPP7385G3): नाव:-कर्ज घेणार - सौ. कल्याणी सोमनाथ पवार वय:-28; पत्ता:-प्लॉट नं: -, माळा नं: -, इमारतीचे नाव: -, ब्लॉक नं: -, रोड नं: स नं 1001, रामोशी वाडी, वेताळनगर, पी एम सी कॉलनी, गोखलेनगर, पुणे, महाराष्ट्र, पुणे. पिन कोड:-411016 पॅन नं:-CNZPP5907P 1): नाव:-कर्ज घेणार - श्रीमती. भारती रामचंद्र पवार वय:-56; पत्ता:-प्लॉट नं: -, माळा नं: -, इमारतीचे नाव: -, ब्लॉक नं: -, रोड नं: स नं 1001, रामोशी वाडी, वेताळनगर, पी एम सी कॉलनी, गोखलेनगर, पुणे , महाराष्ट्र, पुणे. पिन कोड:-411016 पॅन नं:-CANPP1122M 2): नाव:-कर्ज घेणार - श्री. सोमनाथ रामचंद्र पवार वय:-30; पत्ता:-प्लॉट नं: -, माळा नं: -, इमारतीचे नाव: -, ब्लॉक नं: -, रोड नं: स नं 1001, रामोशी वाडी, वेताळनगर, पी एम सी कॉलनी, गोखलेनगर, पुणे, महाराष्ट्र, पुणे. पिन कोड:-411016 पॅन नं:-BPVPP7385G 3): नाव:-कर्ज घेणार - सौ. कल्याणी सोमनाथ पवार वय:-28; पत्ता:-प्लॉट नं: -, माळा नं: -, इमारतीचे नाव: -, ब्लॉक नं: -, रोड नं: स नं 1001, रामोशी वाडी, वेताळनगर, पी एम सी कॉलनी, गोखलेनगर, पुणे, महाराष्ट्र, पुणे. पिन कोड:-411016 पॅन नं:-CNZPP5907P '

    print(parse_party_names(a))

    # s3_client = aws.get_s3_client()
    # aws.download_file_from_s3_bucket(s3_client,bucket,file)
    # bs_obj = get_bs4_html_obj(file)
    # deeds_details = get_html_as_str(reader)
    # print(deeds_details)
    # print(parse_loan_amount(deeds_details),end='\n')
    # print(parse_prop_desc(deeds_details),end='\n')
    # print(parse_stamp_duty(deeds_details),end='\n')
    # print(parse_date_of_filing(deeds_details),end='\n')
    # print(parse_loan_amount(deeds_details),end='\n')
    # print(parse_mortgage_date(deeds_details),end='\n')
    # print(parse_second_party(deeds_details),end='\n')
    # print(parse_first_party(deeds_details),end='\n')
    # print(parse_village(bs_obj),end='\n')
    # print(parse_filing_amount(deeds_details),end='\n')
    # print(parse_date_of_submission(deeds_details),end='\n')
    # print(parse_area(deeds_details),end='\n')

    # parse_village(bs_obj)
    # print('Title')
    # print(parse_title(deeds_details),end='\n')
    # print('--------------')
    # print("Compensation Price")
    # print(parse_compensation(deeds_details),end='\n')
    # print('--------------')
    # print('Market Price')
    # print(parse_market_price(deeds_details),end='\n')
    # print('--------------')
    # print('Prop Desc')
    # print(parse_prop_desc(deeds_details),end='\n')
    # print('--------------')
    # print('Area')
    # print(parse_area(deeds_details),end='\n')
    # print('--------------')
    # print('First party')
    # print(parse_first_party(deeds_details),end='\n')
    # print('--------------')
    # print('Second party')
    # print(parse_second_party(deeds_details),end='\n')
    # print('--------------')
    # print('Date of filing')
    # print(parse_date_of_filing(deeds_details),end='\n')
    # print('--------------')
    # print('Date of exec')
    # print(parse_date_of_execution(deeds_details),end='\n')
    # print('--------------')
    # print('Stamp duty')
    # print(parse_stamp_duty(deeds_details),end='\n')
    # print('--------------')
    # print('Reg fee')
    # print(parse_reg_fee(deeds_details),end='\n')
    # print('--------------')
    # print('Reg date')
    # print(parse_reg_date(deeds_details),end='\n')
    # print('--------------')
    # print('Mortgage date')
    # print(parse_mortgage_date(deeds_details),end='\n')
    # print('--------------')
    # print('Filing amount')
    # print(parse_filing_amount(deeds_details),end='\n')
    # print('--------------')
    # print('Loan amount')
    # print(parse_loan_amount(deeds_details),end='\n')
    # print('--------------')
    # print('Submission amount')
    # print(parse_date_of_submission(deeds_details),end='\n')
