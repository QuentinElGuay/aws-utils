import logging
import os
from tempfile import NamedTemporaryFile

from boto3 import client
from moto import mock_s3
import pytest

from awsutils.s3 import download_object, upload_file


class TestS3Functions:
    @pytest.fixture(scope='function')
    def aws_credentials(self):
        """Mocked AWS Credentials for moto."""
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        os.environ['AWS_SECURITY_TOKEN'] = 'testing'
        os.environ['AWS_SESSION_TOKEN'] = 'testing'
        os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

    @pytest.fixture(scope='function')
    def test_client(self, aws_credentials):
        with mock_s3():
            yield client('s3', region_name='us-east-1')

    @pytest.fixture
    def bucket_name(self):
        return 'my-test-bucket'

    @pytest.fixture
    def test_file(self):
        with NamedTemporaryFile(delete=True, suffix='.txt') as tmp:
            with open(tmp.name, 'w', encoding='UTF-8') as f:
                f.write('Lorem Ipsum')

            yield tmp

    @pytest.fixture
    def s3_test(self, test_client, bucket_name, test_file):
        test_client.create_bucket(Bucket=bucket_name)
        test_client.upload_file(test_file.name, bucket_name, 'file.txt')

        yield

    def test_download_object(
        self, test_client, bucket_name, test_file, tmp_path
    ):

        # Init S3 mock
        test_client.create_bucket(Bucket=bucket_name)
        s3_path = 'path/to/file.txt'
        test_client.upload_file(test_file.name, bucket_name, s3_path)

        # Verify that no previous file exists from previous execution
        object_destination = str(tmp_path / 'file.txt')
        assert not os.path.exists(object_destination)

        # Create real boto3 S3 client and call download_object()
        s3_client = client('s3', region_name='us-east-1')
        download_object(s3_client, bucket_name, s3_path, object_destination)

        # Read the downloaded file to verify that the content of the download
        # file matches the expected one
        with open(object_destination, 'r') as file:
            with open(test_file.name, 'r') as expected_file:
                assert file.read() == expected_file.read()

    def test_upload_object(self, test_client, bucket_name, test_file):

        # Init S3 mock
        test_client.create_bucket(Bucket=bucket_name)
        s3_path = 'path/to/file.txt'

        my_client = client('s3', region_name='us-east-1')

        upload_file(my_client, test_file.name, bucket_name, s3_path)

        # Raise exception when not found
        my_client.head_object(Bucket=bucket_name, Key=s3_path)
