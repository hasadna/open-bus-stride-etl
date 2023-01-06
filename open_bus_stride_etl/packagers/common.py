import os
import boto3
import botocore


ENDPOINT_URL = 'https://s3.eu-west-2.wasabisys.com'
BUCKET_NAME = 'stride'


def get_s3():
    return boto3.client(
        's3',
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=os.environ['WASABI_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['WASABI_SECRET_ACCESS_KEY'],
    )


def is_file_exists(name):
    try:
        get_s3().head_object(Bucket=BUCKET_NAME, Key=name)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise
    return True
