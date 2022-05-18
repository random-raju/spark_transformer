import boto3

def get_s3_client():
    """
    Desc:
        Initializes a S3 bucket object
    Returns:
        S3 bucket object
	"""

    return boto3.client("s3")


def read_file_in_s3_bucket(s3_client, bucket, key):
    """
    Desc:
        Reads a file in s3 bucket
    Params:
        s3_client: S3 object
    Returns:
        string
	"""
    try:
        s3_response = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = s3_response["Body"].read()
        return file_content
    except Exception as e:
        raise e

def download_file_from_s3_bucket(s3, bucket , key):
    """
    Desc:
        Download file from s3 bucket using bucket object
    Params:
        s3_client: S3 bucket object 
    Returns:
        string
    """
    try:
        local_file_name = key
        s3.download_file(Bucket = bucket, Key = key , Filename = local_file_name)
    except Exception as e:
        raise e


if __name__ == "__main__":

    bucket = 'teal-data-pipeline-mh-gom-rd-deeds-pune'
    file = 'पुणे_Haveli 16 (Dhayari)_2020_2_efiling.html'
    s3_client = get_s3_client()
    download_file_from_s3_bucket(s3_client,bucket,file)
    print(read_file_in_s3_bucket(s3_client,bucket,file))

        # match = remove_noise_from_amount(match.strip())
        # value = convert.to_float(match)
        # return value