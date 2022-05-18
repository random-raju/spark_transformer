from dateutil import parser
import json

def to_date(date):

    try:
        date = parser.parse(date.strip(), dayfirst=True)
    except:
        date = None

    return date


def to_jsonb(value):

    if not value:
        return None
    if isinstance(value, str):
        return None
    try:
        value = json.dumps(value)
    except:
        value = None

    return value


def to_float(value):

    if not value:
        return None

    value = value.strip()

    try:
        value = round(float(value),2)
    except:
        value = None

    return value


def to_int(value):

    if not value:
        return None

    value = value.strip()

    try:
        value = int(value)
    except:
        value = None

    return value
