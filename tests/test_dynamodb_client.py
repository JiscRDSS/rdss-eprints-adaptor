import boto3

from datetime import timedelta
from dateutil import parser
from moto import mock_dynamodb2
from app import DynamoDBClient


@mock_dynamodb2
def test_watermark_table():
    # Create the DynamoDB client we'll be testing against
    dynamodb_client = DynamoDBClient(
        'rdss-eprints-adaptor-watermark-test',
        'rdss-eprints-adaptor-processed-test'
    )

    # Create a Boto3 DynamoDB client we'll use to create the mock table
    boto3_client = boto3.client('dynamodb')
    boto3_client.create_table(
        TableName='rdss-eprints-adaptor-watermark-test',
        KeySchema=[
            {
                'AttributeName': 'Key',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'LastUpdated',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Value',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 20,
            'WriteCapacityUnits': 60
        }
    )

    # Verify that a query that returns no rows gives us a 'None' response
    high_watermark = dynamodb_client.fetch_high_watermark()
    assert high_watermark is None

    # Populate a high watermark row into the DynamoDB table
    test_high_watermark_value = parser.parse('2018-03-20T00:00:09')
    dynamodb_client.update_high_watermark(test_high_watermark_value)

    # Verify that we get the correct response, with a second appended to the given high watermark
    high_watermark = dynamodb_client.fetch_high_watermark()
    assert high_watermark == test_high_watermark_value + timedelta(seconds=1)


@mock_dynamodb2
def test_processed_table():
    # Create the DynamoDB client we'll be testing against
    dynamodb_client = DynamoDBClient(
        'rdss-eprints-adaptor-watermark-test',
        'rdss-eprints-adaptor-processed-test'
    )

    # Create a Boto3 DynamoDB client we'll use to create the mock table
    boto3_client = boto3.client('dynamodb')
    boto3_client.create_table(
        TableName='rdss-eprints-adaptor-processed-test',
        KeySchema=[
            {
                'AttributeName': 'Identifier',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'LastUpdated',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Message',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Reason',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Status',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 20,
            'WriteCapacityUnits': 60
        }
    )

    # Verify that a query that returns no rows gives us a 'None' response
    processed_status = dynamodb_client.fetch_processed_status('eprints-identifier-test')
    assert processed_status is None

    # Populate a processed record into the DynamoDB table
    dynamodb_client.update_processed_record('eprints-identifier-test', '{}', 'Success', '-')

    # Verify that we get the correct response
    processed_status = dynamodb_client.fetch_processed_status('eprints-identifier-test')
    assert processed_status == 'Success'
