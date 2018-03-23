import boto3
import json
import time

from app import KinesisClient
from app import PoisonPill
from moto import mock_kinesis


@mock_kinesis
def test_put_message_on_queue():
    # Create the Kinesis client we'll be testing against
    kinesis_client = KinesisClient('rdss-eprints-adaptor-test-stream')

    # Create a Boto3 Kinesis client we'll use to cretae the stream
    client = boto3.client('kinesis')
    client.create_stream(
        StreamName='rdss-eprints-adaptor-test-stream',
        ShardCount=1
    )

    # Get a handle on the test JSON message
    test_message = _get_test_message()

    # Put the test JSON message onto the queue for processing
    kinesis_client.put_message_on_queue(json.dumps(test_message))

    # Now kill the worker and shut down the client down
    kinesis_client.put_message_on_queue(PoisonPill)

    # Just a noddy little loop while we wait for the worker to die...
    while kinesis_client.queue_worker_thread.isAlive():
        time.sleep(0.1)

    # Fetch the message from the stream, to ensure it was added
    shard_id = client.describe_stream(
        StreamName='rdss-eprints-adaptor-test-stream'
    )['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = client.get_shard_iterator(
        StreamName='rdss-eprints-adaptor-test-stream',
        ShardId=shard_id,
        ShardIteratorType='TRIM_HORIZON'
    )['ShardIterator']
    response = client.get_records(
        ShardIterator=shard_iterator
    )

    # Extract the JSON payload and validate it matches the input message
    assert len(response['Records']) == 1
    json_data = json.loads(response['Records'][0]['Data'])
    assert test_message == json_data


def _get_test_message():
    return json.load(open('tests/data/message.json'))
