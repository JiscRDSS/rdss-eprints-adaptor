import boto3
import logging

from datetime import datetime, timedelta
from dateutil import parser


class DynamoDBClient(object):

    def __init__(self, watermark_table_name, processed_table_name):
        self.watermark_table_name = watermark_table_name
        self.procesed_table_name = processed_table_name
        self.client = self._initialise_client()

    def _initialise_client(self):
        logging.info('Initialising Boto3 DynamoDB client')
        return boto3.client('dynamodb')

    def fetch_high_watermark(self):
        logging.info('Fetching high watermark from table [%s]', self.watermark_table_name)
        response = self.client.get_item(
            TableName=self.watermark_table_name,
            Key={
                'Key': {
                    'S': 'HighWatermark'
                }
            }
        )
        if 'Item' in response:
            high_watermark = parser.parse(response['Item']['Value']['S'])
            logging.info('Got high watermark [%s]', high_watermark)
            return high_watermark
        else:
            logging.info('No high watermark exists, this is probably a first run')
            return None

    def update_high_watermark(self, high_watermark):
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
        logging.info(
            'Fetching processed record with EPrints identifier [%s] from table [%s]',
            eprints_identifier,
            self.procesed_table_name
        )
        response = self.client.get_item(
            TableName=self.procesed_table_name,
            Key={
                'Identifier': {
                    'S': eprints_identifier
                }
            }
        )
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
        logging.info(
            'Updating processed record [%s] with a status of [%s] (reason: [%s]) in table [%s]',
            eprints_identifier,
            status,
            reason,
            self.procesed_table_name
        )
        self.client.put_item(
            TableName=self.procesed_table_name,
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
