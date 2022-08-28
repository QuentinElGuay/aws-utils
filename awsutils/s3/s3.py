#!/usr/bin/python3

import logging

def download_object(
    s3_client: object, bucket: str, object_path: str, filename: str) -> object:
    """
    Download in memory an object from an S3 bucket.
    :param s3_client: boto3 client for S3
    :param bucket: Bucket to download the object from
    :param object_path: S3 object path
    """
    try:
        s3_object = s3_client.download_file(
            Bucket=bucket,
            Key=object_path,
            Filename=filename
        )
        logging.info(
            'Object s3://%s/%s downloaded with success.', bucket, object_path)

    except s3_client.exceptions.NoSuchBucket as e:
        logging.error('The specified bucket %s doesn\'t exists.', bucket)
        raise e

    except s3_client.exceptions.NoSuchKey as e:
        logging.error('Object %s not found in bucket %s.', object_path, bucket)
        raise e

    return s3_object


def upload_file(
    s3_client: object, file_path: str, bucket: str, object_path:str) -> None:
    """Upload a file to an S3 bucket
    :param s3_client: boto3 client for S3
    :param file_path: File to upload
    :param bucket: Bucket to upload to
    :param object_path: S3 object name. If not specified then file_name is used
    """

    # Upload the file
    logging.info('Uploading %s to s3://%s/%s.', file_path, bucket, object_path)
    try:
        s3_client.upload_file(file_path, bucket, object_path)
    except Exception as e:
        logging.error(e)
        raise e
