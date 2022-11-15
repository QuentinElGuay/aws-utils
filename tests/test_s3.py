import os
from tempfile import NamedTemporaryFile

from boto3 import client
from moto import mock_s3
import pytest

from awsutils.s3 import InvalidS3URI, download_file, split_s3_uri, upload_file


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

    # @pytest.fixture
    # def s3_test(self, test_client, bucket_name, test_file):
    #     test_client.create_bucket(Bucket=bucket_name)
    #     test_client.upload_file(test_file.name, bucket_name, 'file.txt')

    #     yield


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

        # Create real boto3 S3 client and call download_file()
        s3_client = client('s3', region_name='us-east-1')
        download_file(s3_client, bucket_name, s3_path, object_destination)

        # Read the downloaded file to verify that the content of the download
        # file matches the expected one
        with open(object_destination, 'r') as file:
            with open(test_file.name, 'r') as expected_file:
                assert file.read() == expected_file.read()


    def test_download_object_raise_error(
        self, test_client, bucket_name, test_file, tmp_path
    ):

        # Init S3 mock
        test_client.create_bucket(Bucket=bucket_name)
        s3_path = 'path/to/file.txt'
        object_destination = str(tmp_path / 'file.txt')

        # Create real boto3 S3 client and call download_file()
        s3_client = client('s3', region_name='us-east-1')

        # with pytest.raises(s3_client.exceptions.ClientError):
        try:
            download_file(s3_client, bucket_name, s3_path, object_destination)

        except s3_client.exceptions.ClientError as ex:
            assert ex.response['Error']['Code'] == '404'


    def test_upload_object(self, test_client, bucket_name, test_file):

        # Init S3 mock
        test_client.create_bucket(Bucket=bucket_name)
        s3_path = 'path/to/file.txt'

        s3_client = client('s3', region_name='us-east-1')

        upload_file(s3_client, test_file.name, bucket_name, s3_path)

        # Raise exception when not found
        s3_client.head_object(Bucket=bucket_name, Key=s3_path)


    def test_upload_object_raise_error(
        self, test_client, bucket_name, test_file
    ):
        # Init S3 mock
        test_client.create_bucket(Bucket=bucket_name)
        s3_path = 'path/to/file.txt'

        s3_client = client('s3', region_name='us-east-1')

        upload_file(s3_client, test_file.name, bucket_name, s3_path)

        # Raise exception for bucket not found
        with pytest.raises(s3_client.exceptions.NoSuchBucket):
            s3_client.head_object(Bucket='another_name', Key=s3_path)


    @pytest.mark.parametrize(
        "uri,expected_bucket,expected_key",
        [
            ('s3://bucket/path/to/file.csv', 'bucket', 'path/to/file.csv'),
            ('s3://bucket/path/to/folder/', 'bucket', 'path/to/folder/'),
        ]
    )
    def test_split_s3_uri_OK(self, uri, expected_bucket, expected_key):
        result = split_s3_uri(uri)

        assert expected_bucket == result.bucket
        assert expected_key == result.key
    
    
    def test_split_s3_uri_KO(self):
        with pytest.raises(InvalidS3URI): 
            split_s3_uri('http://bucket/path/to/file.csv')
