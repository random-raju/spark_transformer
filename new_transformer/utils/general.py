import hashlib

def get_hash(string_to_hash):

    hashed = hashlib.md5(string_to_hash.encode()).hexdigest()

    return hashed

def update_reclean_status(value):

    if not value:
        return 1
    else:
        return value + 1