#!/usr/bin/python3

import logging


def execute(
    ath_client: object, query: str, output_bucket: str, output_path: str
):

    query_execution = ath_client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': f's3://{output_bucket}/{output_path}/'
        },
    )

    logging.info(
        'Inicio da query [%s], ID de execução: %s.',
        query,
        query_execution['QueryExecutionId'],
    )

    return query_execution


def get_query_result(ath_client: object, execution_id: str):
    response = ath_client.get_query_execution(QueryExecutionId=execution_id)

    logging.info(
        f'Query %s com o estado %s.',
        execution_id,
        response['QueryExecution']['Status']['State'],
    )

    return response


def read_response(query_execution_response: dict):

    result_dict = {}

    s3_path = query_execution_response['ResultConfiguration']['OutputLocation']

    status = query_execution_response['Status']
    result_dict['estado_query'] = status['State']
    result_dict['inicio_query'] = status['SubmissionDateTime']
    result_dict['fim_query'] = status['SubmissionDateTime']

    result_dict['endereco_arquivo'] = s3_path

    statistics = query_execution_response['Statistics']
    result_dict['tempo_fila_query_ms'] = statistics['QueryQueueTimeInMillis']
    result_dict['tempo_execucao_query_ms'] = statistics[
        'TotalExecutionTimeInMillis'
    ]

    return result_dict
