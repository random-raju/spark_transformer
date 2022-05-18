from indictrans import Transliterator

def transliterate(trans_object, text):
    """
	transliterate the pdf text from hindi to english
	return the complete text in english
	"""
    try:
        translit_text = trans_object.transform(text).title()
    except:
        translit_text = text

    return translit_text


def get_transliterate_object(src):
    """
	Create a Transliterator object
	"""
    try:
        trans = Transliterator(source = src)
    except:
        trans = None

    return trans