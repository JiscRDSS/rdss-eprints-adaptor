import mock
import moto
import boto3
import contextlib
import functools
import responses

from xml.dom import minidom
from urllib.parse import parse_qs

def _get_xml_file(file_path):
    return minidom.parse(file_path).toxml()

class MockResponse(object):

    def __init__(self, response_data, status_code, status_msg):
        self.response_data = response_data
        self.status_code = status_code
        self.status_msg = status_msg
        self.headers = {'Content-Type': 'text/xml; charset=UTF-8'}

    def read(self):
        return self.response_data

    def close(self):
        pass

def mock_oai_response_to_prefix(*args, **kwargs):
    """ Extracts metadataPrefix being used in call to urlopen by the underlying
        oaipmh client and returns appropriate response.
        """
    prefix = parse_qs(args[0].data)[b'metadataPrefix'][0]
    responses = {
        b'ore': MockResponse(_get_xml_file('tests/app/data/ore_response.xml'), 200, 'OK'),
        b'oai_dc': MockResponse(_get_xml_file('tests/app/data/oai_dc_response.xml'), 200, 'OK')
    }
    return responses[prefix]

def setup_mock_kinesis_streams():
    output_stream_name = 'rdss_output_stream_test'
    invalid_stream_name = 'rdss_invalid_stream_test'
    client = boto3.client('kinesis')
    client.create_stream(
        StreamName=output_stream_name,
        ShardCount=1
    )
    client.create_stream(
        StreamName=invalid_stream_name,
        ShardCount=1
    )
    return output_stream_name, invalid_stream_name


def setup_mock_dynamodb_tables():
    watermark_table_name = 'adaptor-watermark-test'
    processed_table_name = 'adaptor-processed-test'
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


def setup_mock_s3_bucket():
    bucket_name = 'adaptor-test'
    conn = boto3.resource('s3')
    conn.create_bucket(Bucket=bucket_name)
    return bucket_name


def mock_oai_pmh_adaptor_infra():
    mocking_managers = [
        (moto.mock_dynamodb2, [], {}),
        (moto.mock_kinesis, [], {}),
        (moto.mock_s3, [], {}),
        (
            mock.patch,
            ['oaipmh.client.urllib2.urlopen'],
            {'side_effect': mock_oai_response_to_prefix},
        ),
    ]

    def decorator(func, *args, **kwargs):
        # `wraps` preserves function info for decorated function e.g. __name__
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # This allows the setup of multiple context managers without lots of nested `withs`
            with contextlib.ExitStack() as stack:
                [
                    stack.enter_context(f(*f_args, **f_kwargs))
                    for f, f_args, f_kwargs in mocking_managers
                ]
                setup_mock_kinesis_streams()
                setup_mock_dynamodb_tables()
                setup_mock_s3_bucket()
                # The following two patches of `responses` are required to deal with an issue
                # where moto will catch and mock all request.get()'s v. 
                # https://github.com/spulec/moto/issues/1026
                responses.add_passthru('https://')
                responses.add_passthru('http://')
                return func(*args, **kwargs)
        return wrapper
    return decorator
