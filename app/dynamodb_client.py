import boto3
import logging

from datetime import datetime, timedelta
from dateutil import parser

import pytest_cov
import pylint
import pre_commit

class DynamoDBClient(object):

    def __init__(self, watermark_table_name, processed_table_name):
        self.watermark_table_name = watermark_table_name
        self.processed_table_name = processed_table_name
        self.client = self._initialise_client()

    def _initialise_client(self):
        logging.info('Initialising Boto3 DynamoDB client')
        return boto3.client('dynamodb')

    def fetch_high_watermark(self):
        # Query DynamoDB to fetch the high watermark. There should only be one row in this table...
        logging.info('Fetching high watermark from table [%s]', self.watermark_table_name)
        response = self.client.get_item(
            TableName=self.watermark_table_name,
            Key={
                'Key': {
                    'S': 'HighWatermark'
                }
            }
        )

        # If this is a "first run", then there won't be any data in the DynamoDB table. So check
        # first.
        if 'Item' in response:
            # The high watermark value should be an ISO8001 compliant string.
            high_watermark = parser.parse(response['Item']['Value']['S'])
            logging.info('Got high watermark [%s]', high_watermark)
            return high_watermark
        else:
            logging.info('No high watermark exists, this is probably a first run')
            return None

    def update_high_watermark(self, high_watermark):
        # Set the high watermark, to be the timestamp given plus 1 second. If we don't add 1
        # second, we'll keep fetching the last record over and over.
        logging.info(
            'Setting high watermark [%s] in table [%s]',
            high_watermark,
            self.watermark_table_name
        )
        self.client.put_item(
            TableName=self.watermark_table_name,
            Item={
                'Key': {
                    'S': 'HighWatermark'
                },
                'Value': {
                    'S': (high_watermark + timedelta(seconds=1)).isoformat()
                },
                'LastUpdated': {
                    'S': datetime.now().isoformat()
                }
            }
        )

    def fetch_processed_status(self, eprints_identifier):
        # Query the DynamoDB table to fetch the status of a record with the given identifier.
        logging.info(
            'Fetching processed record with EPrints identifier [%s] from table [%s]',
            eprints_identifier,
            self.processed_table_name
        )
        response = self.client.get_item(
            TableName=self.processed_table_name,
            Key={
                'Identifier': {
                    'S': eprints_identifier
                }
            }
        )

        # If this identifier has never been seen before, it won't have a row in the DyanmoDB table.
        if 'Item' in response:
            status = response['Item']['Status']['S']
            logging.info(
                'Got processed record status [%s] for EPrints identifier [%s]',
                status,
                eprints_identifier
            )
            return status
        else:
            logging.info(
                'No processed record exists for EPrints identifier [%s]',
                eprints_identifier
            )
            return None

    def update_processed_record(self, eprints_identifier, message, status, reason):
        # Add or update the row in the DynamoDB table with the given idetnfier.
        logging.info(
            'Updating processed record [%s] with a status of [%s] (reason: [%s]) in table [%s]',
            eprints_identifier,
            status,
            reason,
            self.processed_table_name
        )
        self.client.put_item(
            TableName=self.processed_table_name,
            Item={
                'Identifier': {
                    'S': eprints_identifier
                },
                'Message': {
                    'S': message
                },
                'Status': {
                    'S': status
                },
                'Reason': {
                    'S': reason
                },
                'LastUpdated': {
                    'S': datetime.now().isoformat()
                }
            }
        )
