import mock
import moto
import boto3
import contextlib
import functools


def mock_oai_pmh_list_records():
    return

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

def setup_mock_dynamodb_tables():
    watermark_table_name='adaptor-watermark-test'
    processed_table_name='adaptor-processed-test'
    client = boto3.client('kinesis')
    client.create_stream(
            StreamName=output_stream_name,
            ShardCount=1
            )
    client.create_stream(
            StreamName=invalid_stream_name,
            ShardCount=1
            )




def mock_oai_pmh_adaptor_infra():
    mocking_managers = [
        (moto.mock_dynamodb2, [], {}),
        (moto.mock_kinesis, [], {}),
        (moto.mock_s3, [], {}),
        (
            mock.patch,
            ['oaipmh.client.Client.listRecords'],
            {'side_effect': mock_oai_pmh_list_records()},
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
                setup_mock_kinesis_streams(
                        )
                return func(*args, **kwargs)
        return wrapper
    return decorator
