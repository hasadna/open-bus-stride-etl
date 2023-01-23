import os
import time
import traceback

import boto3
import datetime
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


def get_file_last_modified(name) -> datetime.datetime:
    try:
        return get_s3().head_object(Bucket=BUCKET_NAME, Key=name)['LastModified']
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return None
        else:
            raise


def upload_file(filename, key):
    get_s3().upload_file(filename, BUCKET_NAME, key)
    return os.path.join(ENDPOINT_URL, BUCKET_NAME, key)


def download_file(key, filename):
    get_s3().download_file(BUCKET_NAME, key, filename)


def download_legacy_file(bucket_name, key, filename, retries=None):
    if retries is None:
        try:
            get_s3().download_file(bucket_name, key, filename)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise
        return True
    else:
        for i in range(retries):
            if i > 0:
                time.sleep((i*i)/3)
            try:
                if download_legacy_file(bucket_name, key, filename, retries=None):
                    return True
            except Exception:
                traceback.print_exc()
            print(f'failed to download {key} from {bucket_name}, retrying... ({i+1}/{retries})')
        return False


def iterate_keys(bucket_name, key_prefix):
    paginator = get_s3().get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=key_prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Size'] > 0:
                    yield obj['Key']
