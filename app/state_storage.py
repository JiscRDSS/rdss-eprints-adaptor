import boto3
import logging
import datetime
import dateutil.parser

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class AdaptorStateStore(object):
    HIGH_WATERMARK_KEY = 'HighWatermark'

    def __init__(self, watermark_table_name, processed_table_name):
        """ Initialises the AdaptorStateStore with the name of the dynamodb
            tables used to store the high watermark of the adaptor, and the
            set of records that have already been processed.
            :watermark_table_name: String
            :processed_table_name: String
            """
        self.watermark_table = self._init_dynamodb_table(watermark_table_name)
        self.processed_table = self._init_dynamodb_table(processed_table_name)

    def _init_dynamodb_table(self, table_name):
        try:
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table(table_name)
            table.load()
            logger.info('Successfully initialised connection to '
                        '%s', table_name)
            return table
        except ClientError:
            logger.exception('%s initialisation failed.', table_name)

    def put_record_state(self, record_state):
        """ Attempts to put a RecordState into the AdaptorStateStore.
            Overwrites any existing RecordState with the same id in the
            store.
            :record_state: RecordState
            """
        try:
            logger.info('Putting RecordState for %s into %s.',
                        record_state.pure_uuid, self.processed_table.name)
            self.processed_table.put_item(Item=record_state.json)
        except ClientError:
            logger.exception('Unable to put %s into %s.',
                             record_state.json, self.processed_table.name)

    def get_record_state(self, oai_pmh_identifier):
        """ Attempts to retrieve by oai_pmh_identifier the state of a record from the
            AdaptorStateStore. If a matching record is not found an
            empty RecordState is returned.
            :oai_pmh_identifier: String
            :returns: RecordState
            """
        try:
            logger.info('Getting RecordState for %s from %s.',
                        oai_pmh_identifier, self.processed_table.name)
            response = self.processed_table.get_item(
                Key={'Identifier': oai_pmh_identifier})
            item = response.get('Item', {})
            return RecordState(item)
        except ClientError:
            logger.exception('Unable to get RecordState for %s from %s.',
                             oai_pmh_identifier, self.processed_table.name)

    def get_high_watermark(self):
        """ Retrieves the datetime of the most recently modified record
            object from previous runs of the adaptor.
            :returns: DateTime
            """
        try:
            logger.info(
                'Retrieving high watermark from %s.', self.watermark_table.name)
            response = self.watermark_table.get_item(
                Key={'Key': self.HIGH_WATERMARK_KEY})
            date_string = response.get('Item', {}).get('Value')
            if date_string:
                return dateutil.parser.parse(date_string)
            else:
                return None
        except ClientError:
            logger.exception(
                'Unable to get a high watermark from %s.', self.watermark_table.name)

    def update_high_watermark(self, high_watermark_datetime):
        """ Sets the provided Date time string as the high watermark in the state store.
            :high_watermark_datetime: String
            """
        try:
            logger.info('Setting high watermark as %s',
                        high_watermark_datetime)
            self.watermark_table.put_item(Item={
                'Key': self.HIGH_WATERMARK_KEY,
                'Value': high_watermark_datetime,
                'LastUpdated': datetime.datetime.now().isoformat()
            })
        except ClientError:
            logger.exception('Unable to put %s into %s.',
                             high_watermark_datetime, self.watermark_table.name)


class RecordState(object):

    def __init__(self, state_json):
        """ Initialise a RecordState object with the response from the
            dynamodb AdaptorStateStore.
            """
        self.json = state_json

    @property
    def oai_pmh_identifier(self):
        return self.json['Identifier']

    @property
    def last_updated(self):
        return self.json['LastUpdated']

    @property
    def successful_create(self):
        """ Used to determine if this RecordState represents a valid MetadataCreate message,
            if so, the next message will be a MetadataUpdate, otherwise MetadataCreate
            should be re-attempted.
            """
        status = self.json.get('Status')
        message_type = self.json.get('Message').get(
            'messageHeader').get('messageType')
        if status == 'Success' and message_type == 'MetadataCreate':
            return True
        else:
            return False

    @classmethod
    def create_from_record(cls, record):
        """ Initialise a RecordState object with a OAIPMHRecord object.
            Creates a dummy messageBody for comparison.
            """
        record_json = {
            'Identifier': record.oai_pmh_identifier,
            'LastUpdated': record.modified_date.isoformat(),
            'Message': {
                'messageBody': record.rdss_canonical_metadata
            },
        }
        return cls(record_json)

    @classmethod
    def create_from_error(cls, identifier, update_date, error_string):
        record_json = {
            'Identifier': identifier,
            'LastUpdated': update_date.isoformat(),
            'Message': {},
            'Status': 'Error',
            'Reason': error_string
        }
        return cls(record_json)

    def update_with_message(self, message):
        self.json.update({
            'Message': message.as_json,
            'Status': 'Success' if message.is_valid else 'Invalid',
            'Reason': ' - '.join(message.error_info)
        })

    @property
    def message_body(self):
        """ Extract and return the messageBody from the last generated message
            for this record. Used to determine whether the modification of the
            record in OAI-PMH provider has changed the RDSS CDM manifestation
            of a record and whether an UPDATE message should be generated. If
            no messageBody then will return None to indicate a CREATE message
            should be generated.
            :return: dict
            """
        return self.json.get('Message', {}).get('messageBody')

    @property
    def object_uuid(self):
        """ Extract and return the objectUUID from the stored message.
            Used to effect object versioning as defined in the message api spec."""
        return self.json.get('Message', {}).get('messageBody', {}).get('objectUuid')

    def __eq__(self, other):
        return (self.message_body == other.message_body)

    def __ne__(self, other):
        return (self.message_body != other.message_body)
