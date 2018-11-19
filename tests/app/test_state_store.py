import boto3
import copy
import moto
import pytest
import dateutil
from collections import namedtuple

from app.state_storage import AdaptorStateStore, RecordState

MockMessage = namedtuple(
    'Message', [
        'as_json',
        'is_valid',
        'error_info'
    ]
)

MockRecord = namedtuple(
    'Record', [
        'oai_pmh_identifier',
        'modified_date',
        'rdss_canonical_metadata'
    ]
)


@pytest.fixture
def modified_date():
    return dateutil.parser.parse('2016-07-05T15:53:57.883+0000')


@pytest.fixture
def mock_record(modified_date):
    return MockRecord('a_mock_oai_pmh_identifier', modified_date, {})


@pytest.fixture
def mock_valid_message():
    return MockMessage({'messageBody': {'objectTitle': 'Test Title'}}, True, ('', ''))


@pytest.fixture
def mock_invalid_message():
    return MockMessage({'messageBody': {}}, False, ('ERRORCODE', 'An error message'))


@pytest.fixture
def record_state(mock_record):
    return RecordState.create_from_record(mock_record)


@moto.mock_dynamodb2
def setup_dynamodb_tables():
    watermark_table_name = 'watermark_table'
    processed_table_name = 'processed_table'
    ddb = boto3.resource('dynamodb')
    ddb.create_table(
        TableName=watermark_table_name,
        KeySchema=[{'AttributeName': 'Key', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'Key', 'AttributeType': 'S'}],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10}
    )
    ddb.create_table(
        TableName=processed_table_name,
        KeySchema=[{'AttributeName': 'Identifier', 'KeyType': 'HASH'}],
        AttributeDefinitions=[
            {'AttributeName': 'Identifier', 'AttributeType': 'S'}],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10}
    )
    return watermark_table_name, processed_table_name


@moto.mock_dynamodb2
def test_adaptor_state_store(record_state):
    state_store = AdaptorStateStore(*setup_dynamodb_tables())
    state_store.put_record_state(record_state)
    store_record_state = state_store.get_record_state(
        record_state.oai_pmh_identifier)
    assert record_state.json == store_record_state.json
    assert record_state == store_record_state


@moto.mock_dynamodb2
def test_adaptor_state_store_latest(modified_date):
    state_store = AdaptorStateStore(*setup_dynamodb_tables())
    state_store.update_high_watermark(modified_date.isoformat())
    latest_datetime = state_store.get_high_watermark()
    assert latest_datetime == modified_date


def test_record_update_with_message(record_state, mock_valid_message):
    original_state = copy.deepcopy(record_state)
    record_state.update_with_message(mock_valid_message)
    assert original_state != record_state


def test_record_state_success(record_state, mock_valid_message):
    record_state.update_with_message(mock_valid_message)
    assert record_state.json['Status'] == 'Success'
    assert record_state.json['Reason'] == ' - '


def test_record_state_invalid(record_state, mock_invalid_message):
    record_state.update_with_message(mock_invalid_message)
    assert record_state.json['Status'] == 'Invalid'
    assert record_state.json['Reason'] == 'ERRORCODE - An error message'
