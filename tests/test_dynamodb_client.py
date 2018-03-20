import boto3

from datetime import timedelta
from dateutil import parser
from moto import mock_dynamodb2
from app import DynamoDBClient

import pytest_cov
import pylint

test_high_watermark_last_updated = parser.parse('2018-03-20T08:48:04')
test_high_watermark_value = parser.parse('2018-03-20T00:00:09')


@mock_dynamodb2
def test_fetch_high_watermark():
    # Create the DynamoDB client we'll be testing again
    dynamodb_client = DynamoDBClient(
        'rdss-eprints-adaptor-watermark-test',
        'rdss-eprints-adaptor-processed-test'
    )

    # Create a Boto3 DynamoDB client we'll use to create the mock table and populate it
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
    dynamodb_client.update_high_watermark(test_high_watermark_value)

    # Verify that we get the correct response, with a second appended to the given high watermark
    high_watermark = dynamodb_client.fetch_high_watermark()
    assert high_watermark is not None
    assert high_watermark == test_high_watermark_value + timedelta(seconds=1)
