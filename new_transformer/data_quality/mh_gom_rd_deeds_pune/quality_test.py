import re

def check_pan(pan_numbers):

    if not pan_numbers:
        return True
    pan_numbers = pan_numbers.split('|')

    for pan in pan_numbers:
        if not pan:
            continue
        if len(pan) != 10:
            return False
        if not re.search(r'[A-Z]{5}\d{4}[A-Z]'):
            return False
    
    return True
