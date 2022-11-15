import logging
from dataclasses import dataclass
import re

import botocore


class InvalidS3URI(Exception):
    pass


@dataclass
class S3Object:
    bucket: str
    key: str


def split_s3_uri(s3_uri: str) -> S3Object:
    """Split an S3 URI to return a bucket and a key.

    Args:
        s3_uri (str): S3 URI in the s3://{bucket}/{key} format.

    Raises:
        InvalidS3URI: The URI passed in argument doesn't match the S3 URI 
            pattern.

    Returns:
        S3Object: a simple object containing the bucket and key corresponding 
            to the URI.
    """
    s3_uri_pattern = r'^s3://([^/]+)/(.*?/?)$'
    match = re.search(s3_uri_pattern, s3_uri)
    
    if not match:
        raise InvalidS3URI(f'{s3_uri} doesn\'t match the S3 URI pattern.')

    bucket = match.groups()[0]
    key = match.groups()[1]
    return S3Object(bucket, key)



def download_file(
    client: botocore.client.BaseClient,
    bucket: str,
    object_key: str,
    file_name: str
):
    """Download and object from S3.

    Args:
        client (botocore.client.S3): S3 Client from boto3.
        bucket (str): Name of the bucket to download from.
        object_key (str): Key of the object to download.
        file_name (str): Path to the local file system to store the downloaded
            object to.
    """

    logging.info('Downloading file s3://%s/%s', bucket, object_key)
    object = client.download_file(
        Bucket=bucket,
        Key=object_key,
        Filename=file_name
    )
    logging.debug(f'Object downloaded to %s.', file_name)

    return object


def upload_file(
    client: botocore.client.BaseClient,
    file_name: str,
    bucket: str,
    object_key:str
):
    """Upload a file to an S3 bucket
    Args:
        client (botocore.client.S3): S3 Client from boto3.
        file_name (str): Name of the file to upload.
        bucket (str): Name of the destination bucket.
        object_key (str): Destination key in the bucket.
    """

    logging.info(f'Uploading object {file_name} to s3://{object_key}.')

    client.upload_file(file_name, bucket, object_key)

    logging.debug(f'Object uploaded.')
