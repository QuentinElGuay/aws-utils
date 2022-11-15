from dataclasses import dataclass
import datetime
import logging

import botocore

@dataclass
class QueryResult:
    state: str
    output_location: str
    submission_time: datetime
    completion_time: datetime
    queue_time_ms: int
    execution_time_ms: int


def execute_query(
    client: botocore.client.BaseClient,
    query: str,
    output_bucket: str,
    output_path: str
):
    """Execute an SQL query in AWS Athena.

    Args:
        client (botocore.client.Athena): Athena client from boto3.
        query (str): Query to execute.
        output_bucket (str): Name of the bucket to store the query result.
        output_path (str): Key of the query result in the bucket.

    Returns:
        str: ID of the query execution.
    """

    logging.info('Executing query %s.', query)

    query_execution = client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': f's3://{output_bucket}/{output_path}/'
        },
    )

    logging.info(
        'Query execution ID [%s].',
        query_execution['QueryExecutionId']
    )

    return query_execution['QueryExecutionId']


def get_query_result(
    client: botocore.client.BaseClient,
    execution_id: str
):
    """Call the AWS Athena API to obtain the result of a query.

    Args:
        client (botocore.client.Athena): Athena client from boto3.
        execution_id (str): Execution ID to interrogate.

    Returns:
        _type_: _description_
    """
    response = read_response(
        client.get_query_execution(QueryExecutionId=execution_id)
    )

    logging.info(
        f'Query %s with status %s.',
        execution_id,
        response.state
    )

    return response


def read_response(query_execution_response: dict) -> QueryResult:
    """Analyse the result of an Athena query.

    Args:
        query_execution_response (dict): query to analyse.

    Returns:
        QueryResult: Status of the query.
    """
    output = query_execution_response['ResultConfiguration']['OutputLocation']

    status = query_execution_response['Status']
    state = status['State']
    submission_time = status['SubmissionDateTime']
    completion_time = status['CompletionDateTime']

    statistics = query_execution_response['Statistics']
    queue_time_ms = statistics['QueryQueueTimeInMillis']
    execution_time_ms = statistics['TotalExecutionTimeInMillis']

    result = QueryResult(
        state=state,
        output_location=output,
        submission_time=submission_time,
        completion_time=completion_time,
        execution_time_ms=execution_time_ms,
        queue_time_ms=queue_time_ms
    )

    return result
