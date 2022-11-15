import logging

import botocore


def get_partition(
    client: botocore.client.BaseClient,
    database: str,
    table: str,
    partition_values: list[str]
):
    """Access the partition information of an Athena table.

    Args:
        client (botocore.client.Glue): Glue Client from boto3.
        database (str): Database of the targeted table.
        table (str): Name of the targeted table.
        partition_values (list[str]): the values that define a partition.

    Returns:
        dict: dictionary containing the partition informations.
    """
    try:
        return client.get_partition(
            DatabaseName=database,
            TableName=table,
            PartitionValues=partition_values
        )
    except client.exceptions.EntityNotFoundException as error:
        return None


def get_table_definition(
    client: botocore.client.BaseClient,
    database: str,
    table: str
) -> dict:
    """Return the definition of a table from the Data Catalog.

    Args:
        client (botocore.client.Glue): Glue Client from boto3.
        database (str): Database of the targeted table.
        table (str): Name of the targeted table.

    Returns:
        dict: dictionary containing the table definition.
    """
    return client.get_table(
        DatabaseName=database,
        Name=table
    )['Table']


def add_partition(
    client: botocore.client.BaseClient,
    database: str,
    table: str,
    partitions: list[tuple[str]]
):
    """Add a partition to an Athena table.

    Args:
        client (botocore.client.Glue): Glue Client from boto3.
        database (str): Database of the targeted table.
        table (str): Name of the targeted table.
        partitions (list[tuple[str]]): list (key, value) of the partion to add.
    """
    logging.info(
        'Add partition %s to table %s.%s.', partitions, database, table)
    
    response = get_table_definition(client, database, table)
    descriptor = response['StorageDescriptor']

    location_parts = [f'{part[0]}={part[1]}' for part in partitions]
    location = '/'.join([descriptor['Location']] + location_parts)

    input_dict = {
        'Values': [part[1] for part in partitions],
        'StorageDescriptor': {
            'Location': location,
            'InputFormat': descriptor['InputFormat'],
            'OutputFormat': descriptor['OutputFormat'],
            'SerdeInfo': descriptor['SerdeInfo']
        }
    }

    try:
        client.create_partition(
            DatabaseName=database,
            TableName=table,
            PartitionInput=input_dict
        )

        logging.debug('Partitition added with success.')

    except client.exceptions.AlreadyExistsException:
        logging.warning('The partition "%s" already exists.', partitions)
