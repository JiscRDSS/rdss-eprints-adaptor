import json
import re
import requests_mock

from app import MessageGenerator
from dateutil import parser

# https://github.com/JiscRDSS/rdss-message-api-specification/blob/master/schemas/types.json#L11
uuid4_regex = re.compile(
    '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
)


@requests_mock.mock()
def test_generate_metadata_create(*args):
    # Get a handle on the mocker - see https://github.com/pytest-dev/pytest/issues/2749
    requests_mocker = args[0]

    # Mock out the request to the EC2 metadata API to fetch the local IPv4 address
    requests_mocker.get(
        'http://169.254.169.254/2016-09-02/meta-data/local-ipv4',
        content=b'123.123.123.123'
    )

    # Create the message generator client we'll be testing against
    message_generator = MessageGenerator()

    # Generate the message using the dummy values
    test_record = _build_test_record()
    test_s3_object = _build_test_s3_objects()
    message = message_generator.generate_metadata_create(test_record, test_s3_object)

    # Convert the message to a JSON object
    message_json = json.loads(message)

    # Validate the messageId is present and in the correct format
    assert uuid4_regex.match(message_json['messageHeader']['messageId'])

    # Validate the messageTimings are present and in the correct format
    assert parser.parse(message_json['messageHeader']['messageTimings']['publishedTimestamp'])

    # Validate the messageHistory is present and in the correct format
    assert message_json['messageHeader']['messageHistory'][0]['machineAddress'] == '123.123.123.123'
    assert parser.parse(message_json['messageHeader']['messageHistory'][0]['timestamp'])

    # Validate the objectUuid is present and in the correct format
    assert uuid4_regex.match(message_json['messageBody']['objectUuid'])

    # Validate the objectTitle is present and in the correct format
    assert message_json['messageBody']['objectTitle'] == 'Test title'

    # Validate the objectPersonRole is present and in the correct format
    assert message_json['messageBody']['objectPersonRole'][0]['person'][
               'personGivenName'] == 'Test creator'

    # Validate the objectDate is present and in the correct format
    assert parser.parse(message_json['messageBody']['objectDate'][0]['dateValue'])

    # Validate the objectCategory is present and in the correct format
    assert message_json['messageBody']['objectCategory'][0] == 'Test subject'

    # Validate the objectFile is present and in the correct format
    assert uuid4_regex.match(message_json['messageBody']['objectFile'][0]['fileUuid'])
    assert message_json['messageBody']['objectFile'][0]['fileIdentifier'] == 'download/file.dat'
    assert message_json['messageBody']['objectFile'][0]['fileName'] == 'file.dat'
    assert message_json['messageBody']['objectFile'][0]['fileSize'] == 17280
    assert uuid4_regex.match(
        message_json['messageBody']['objectFile'][0]['fileChecksum'][0]['checksumUuid']
    )
    assert message_json['messageBody']['objectFile'][0]['fileChecksum'][0][
               'checksumValue'] == '0c9a2690b41be2660db2aadad13dbf05'
    assert message_json['messageBody']['objectFile'][0][
               'fileStorageLocation'] == 'https://rdss-prints-adaptor-test-bucket.s3.amazonaws.co' \
                                         'm/download/file.dat'


def _build_test_record():
    return {
        'header': {
            'identifier': 'test-eprints-record',
            'datestamp': '2018-03-23T12:34:56'
        },
        'metadata': {
            'title': ['Test title'],
            'creator': ['Test creator'],
            'description': ['Test description'],
            'date': ['2018-03-23T09:10:15'],
            'subject': ['Test subject'],
            'identifier': ['http://eprints.test/download/file.dat']
        }
    }


def _build_test_s3_objects():
    return [{
        'file_name': 'file.dat',
        'file_path': 'download/file.dat',
        'file_size': 17280,
        'file_checksum': '0c9a2690b41be2660db2aadad13dbf05',
        'download_url': 'https://rdss-prints-adaptor-test-bucket.s3.amazonaws.com/download/file.dat'
    }]
