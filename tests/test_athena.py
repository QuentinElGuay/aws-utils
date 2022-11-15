from datetime import datetime
from unittest import mock

from boto3 import client
from moto import mock_athena
from moto.athena import athena_backends
import pytest

from awsutils.athena import execute_query, get_query_result, read_response

class TestAthenaFunctions:


    @pytest.fixture
    def query_execution_id(self):
        yield '78f82692-4fd8-4ac8-9961-7a7d8e8b7ce6'

    @pytest.fixture
    def athena_response(self, query_execution_id):
        response =  {
            'QueryExecutionId': query_execution_id,
            'Query': 'string',
            'StatementType': 'DML',
            'ResultConfiguration': {
                'OutputLocation': 'string',
                'EncryptionConfiguration': {
                    'EncryptionOption': 'SSE_S3',
                    'KmsKey': 'string'
                },
                'ExpectedBucketOwner': 'string',
                'AclConfiguration': {
                    'S3AclOption': 'BUCKET_OWNER_FULL_CONTROL'
                }
            },
            'ResultReuseConfiguration': {
                'ResultReuseByAgeConfiguration': {
                    'Enabled': False,
                    'MaxAgeInMinutes': 123
                }
            },
            'QueryExecutionContext': {
                'Database': 'string',
                'Catalog': 'string'
            },
            'Status': {
                'State': 'SUCCEEDED',
                'StateChangeReason': 'string',
                'SubmissionDateTime': datetime(2015, 1, 1),
                'CompletionDateTime': datetime(2015, 1, 1),
                'AthenaError': {
                    'ErrorCategory': 123,
                    'ErrorType': 123,
                    'Retryable': False,
                    'ErrorMessage': 'string'
                }
            },
            'Statistics': {
                'EngineExecutionTimeInMillis': 123,
                'DataScannedInBytes': 123,
                'DataManifestLocation': 'string',
                'TotalExecutionTimeInMillis': 123,
                'QueryQueueTimeInMillis': 123,
                'QueryPlanningTimeInMillis': 123,
                'ServiceProcessingTimeInMillis': 123,
                'ResultReuseInformation': {
                    'ReusedPreviousResult': False
                }
            },
            'WorkGroup': 'string',
            'EngineVersion': {
                'SelectedEngineVersion': 'string',
                'EffectiveEngineVersion': 'string'
            },
            'ExecutionParameters': [
                'string',
            ]
        }

        yield response


    def test_read_response(self, athena_response):
        
        result = read_response(athena_response)
        status = athena_response['Status']
        statistics = athena_response['Statistics']

        assert result.state == status['State']
        assert result.submission_time == status['SubmissionDateTime']
        assert result.completion_time == status['CompletionDateTime']
        
        assert result.queue_time_ms == statistics['QueryQueueTimeInMillis']
        assert result.execution_time_ms == statistics[
            'TotalExecutionTimeInMillis']

        assert result.output_location == athena_response[
            'ResultConfiguration']['OutputLocation']


    def test_execute_query(self, query_execution_id):
        mock_client = mock.Mock()
        mock_client.start_query_execution.return_value = {
            'QueryExecutionId': query_execution_id,
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HTTPHeaders': {'server': 'amazon.com'},
                'RetryAttempts': 0
            }
        }

        result = execute_query(
             mock_client,
            'SELECT * FROM database.table;',
            'bucket',
            'path_to_file'
        )

        assert result == query_execution_id
        mock_client.start_query_execution.assert_called_once()


    def test_get_query_result(self, query_execution_id, athena_response):
        mock_client = mock.Mock()
        mock_client.get_query_execution.return_value = athena_response

        result = get_query_result(mock_client, query_execution_id)

        mock_client.get_query_execution.assert_called_once_with(
            QueryExecutionId=query_execution_id)
        assert result == read_response(athena_response)
