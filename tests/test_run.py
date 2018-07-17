import json
import os
import run

from app import OAIPMHClient
from app import DownloadClient
from app import DynamoDBClient
from app import KinesisClient
from app import MessageGenerator
from app import MessageValidator
from app import PoisonPill
from app import S3Client
from dateutil import parser
from mock import MagicMock, patch


@patch('run._initialise_download_client')
@patch('run._initialise_dynamodb_client')
@patch('run._initialise_oai_pmh_client')
@patch('run._initialise_kinesis_client')
@patch('run._initialise_message_generator')
@patch('run._initialise_message_validator')
@patch('run._initialise_s3_client')
def test_main(_initialise_s3_client, _initialise_message_validator, _initialise_message_generator,
              _initialise_kinesis_client, _initialise_oai_pmh_client, _initialise_dynamodb_client,
              _initialise_download_client):
    # Initialise the test environment variables
    _initialise_env_variables()

    # Mock out the download client
    mock_download_client = _mock_download_client()
    _initialise_download_client.return_value = mock_download_client

    # Mock out the DynamoDB client
    mock_dynamodb_client = _mock_dynamodb_client()
    _initialise_dynamodb_client.return_value = mock_dynamodb_client

    # Mock out the OAI PMH client
    mock_oai_pmh_client = _mock_oai_pmh_client()
    _initialise_oai_pmh_client.return_value = mock_oai_pmh_client

    # Mock out the Kinesis client
    mock_kinesis_client = _mock_kinesis_client()
    _initialise_kinesis_client.return_value = mock_kinesis_client

    # Mock out the message generator
    mock_message_generator = _mock_message_generator()
    _initialise_message_generator.return_value = mock_message_generator

    # Mock out the message validator
    mock_message_validator = _mock_message_validator()
    _initialise_message_validator.return_value = mock_message_validator

    # Mock out the S3 client
    mock_s3_client = _mock_s3_client()
    _initialise_s3_client.return_value = mock_s3_client

    # Execute the main function
    run.main()

    # Validate that the appropriate calls were made
    mock_dynamodb_client.fetch_high_watermark.assert_called_once_with()
    mock_oai_pmh_client.fetch_records_from.assert_called_once_with('1970-01-01T00:00:00')
    mock_dynamodb_client.fetch_processed_status.assert_called_once_with('test-identifier')
    mock_download_client.download_file.assert_called_once_with(
        'http://eprints.test/download/file.dat'
    )
    mock_s3_client.push_to_bucket.assert_called_once_with(
        'http://eprints.test/download/file.dat',
        '/path/to/file.dat'
    )
    mock_message_generator.generate_metadata_create.assert_called_once_with(
        {
            'identifier': 'test-identifier',
            'datestamp': parser.parse('2004-02-16T14:10:55'),
            'oai_dc': {
                'creator': ['Test Creator'],
                'contributor': ['Test Contributor'],
                'date': ['2004-02-16T13:51:07Z'],
                'identifier': ['http://eprints.test/download/file.dat'],
                'description': ['Test Description'],
                'language': ['en_GB'],
                'subject': ['Test Subject'],
                'title': ['Test Title'],
                'type': ['Test Type'],
                'format': ['Test Format']
            },
            'file_locations': ['http://eprints.test/download/file.dat']
        },
        [{
            'file_name': 'file.dat',
            'file_path': 'download/file.dat',
            'file_size': 17280,
            'file_checksum': '0c9a2690b41be2660db2aadad13dbf05',
            'download_url': 'https://rdss-prints-adaptor-test-bucket.s3.amazonaws.com/download/fil'
                            'e.dat'
        }]
    )
    assert mock_kinesis_client.put_message_on_queue.call_count == 2
    mock_dynamodb_client.update_high_watermark.assert_called_once_with(
        parser.parse('2004-02-16T14:10:55')
    )


def _initialise_env_variables():
    os.environ['OAI_PMH_ENDPOINT_URL'] = 'http://eprints.test/cgi/oai2'
    os.environ['OAI_PMH_PROVIDER'] = 'eprints'
    os.environ['JISC_ID'] = '12345'
    os.environ['ORGANISATION_NAME'] = 'Test Organisation'
    os.environ['DYNAMODB_WATERMARK_TABLE_NAME'] = 'rdss-eprints-adaptor-watermark-test'
    os.environ['DYNAMODB_PROCESSED_TABLE_NAME'] = 'rdss-eprints-adaptor-processed-test'
    os.environ['S3_BUCKET_NAME'] = 'rdss-prints-adaptor-test-bucket'
    os.environ['OUTPUT_KINESIS_STREAM_NAME'] = 'rdss-eprints-adaptor-test-stream'
    os.environ['OUTPUT_KINESIS_INVALID_STREAM_NAME'] = 'rdss-eprints-adaptor-invalid-stream'
    os.environ['RDSS_MESSAGE_API_SPECIFICATION_VERSION'] = '3.0.1'
    os.environ['OAI_PMH_ADAPTOR_FLOW_LIMIT'] = '1'


def _mock_download_client():
    mock_download_client = DownloadClient()
    mock_download_client.download_file = MagicMock(return_value='/path/to/file.dat')
    return mock_download_client


def _mock_dynamodb_client():
    mock_dynamodb_client = DynamoDBClient(
        'rdss-eprints-adaptor-processed-test',
        'rdss-eprints-adaptor-watermark-test'
    )
    mock_dynamodb_client.fetch_high_watermark = MagicMock(return_value='1970-01-01T00:00:00')
    mock_dynamodb_client.update_high_watermark = MagicMock(return_value=None)
    mock_dynamodb_client.fetch_processed_status = MagicMock(return_value=None)
    mock_dynamodb_client.update_processed_record = MagicMock(return_value=None)
    return mock_dynamodb_client


def _mock_oai_pmh_client():
    mock_oai_pmh_client = OAIPMHClient('http://eprints.test/cgi/oai2')
    mock_oai_pmh_client.fetch_records_from = MagicMock(
        return_value=[
            {
                'identifier': 'test-identifier',
                'datestamp': parser.parse('2004-02-16T14:10:55'),
                'oai_dc': {
                    'creator': ['Test Creator'],
                    'contributor': ['Test Contributor'],
                    'date': ['2004-02-16T13:51:07Z'],
                    'identifier': ['http://eprints.test/download/file.dat'],
                    'description': ['Test Description'],
                    'language': ['en_GB'],
                    'subject': ['Test Subject'],
                    'title': ['Test Title'],
                    'type': ['Test Type'],
                    'format': ['Test Format']
                },
                'file_locations': ['http://eprints.test/download/file.dat'],
            }
        ]
    )
    return mock_oai_pmh_client


def _mock_kinesis_client():
    mock_kinesis_client = KinesisClient(
        'rdss-eprints-adaptor-test-stream',
        'rdss-eprints-adaptor-invalid-stream'
    )
    mock_kinesis_client.put_message_on_queue(PoisonPill)
    mock_kinesis_client.put_message_on_queue = MagicMock(return_value=None)
    mock_kinesis_client.put_invalid_message_on_queue = MagicMock(return_value=None)
    return mock_kinesis_client


def _mock_message_generator():
    mock_message_generator = MessageGenerator(12345, 'Test Organisation', 'dspace')
    mock_message_generator.generate_metadata_create = MagicMock(
        return_value=json.dumps(json.load(open('tests/app/data/rdss-message.json')))
    )
    return mock_message_generator


def _mock_message_validator():
    mock_message_validator = MessageValidator('3.0.1')
    mock_message_validator._download_model_schemas = MagicMock(return_value=None)
    mock_message_validator._download_message_schema = MagicMock(return_value=None)
    mock_message_validator.validate_message = MagicMock(return_value=None)
    return mock_message_validator


def _mock_s3_client():
    mock_s3_client = S3Client('rdss-prints-adaptor-test-bucket')
    mock_s3_client.push_to_bucket = MagicMock(
        return_value={
            'file_name': 'file.dat',
            'file_path': 'download/file.dat',
            'file_size': 17280,
            'file_checksum': '0c9a2690b41be2660db2aadad13dbf05',
            'download_url': 'https://rdss-prints-adaptor-test-bucket.s3.amazonaws.com/download/fil'
                            'e.dat'
        }
    )
    return mock_s3_client
