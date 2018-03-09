import boto3
import logging
import sys
import time
import uuid

from queue import Queue
from threading import Thread


class KinesisClient(object):

    def __init__(self, stream_name):
        self.stream_name = stream_name
        self.message_queue = Queue()
        self.client = self._initialise_client()
        self._initialise_queue_worker()

    def _initialise_client(self):
        logging.info('Initialising Boto3 Kinesis client')
        return boto3.client('kinesis')

    def _initialise_queue_worker(self):
        try:
            queue_worker = Thread(target=self._process_queue, name='KinesisQueueWorker')
            logging.info('Starting Kinesis queue worker [%s]', queue_worker)
            queue_worker.start()
        except:
            logging.exception('An error occurred initialising the Kinesis queue worker')
            sys.exit(-1)

    def put_message_on_queue(self, message):
        logging.info('Adding message [%s] to the queue', message)
        self.message_queue.put_nowait(message)

    def _process_queue(self):
        while True:
            logging.debug('Sleeping for [0.5] seconds before processing next message on queue')
            time.sleep(0.5)
            message = self._fetch_message_from_queue()
            logging.debug('Got message [%s] from queue', message)
            if message is not None:
                self._put_message_to_stream(message)

    def _fetch_message_from_queue(self):
        if self.message_queue.empty():
            logging.debug('No messages on queue to process')
            return None
        else:
            logging.info('Fetching message from queue ([%s] remaining)', self.message_queue.qsize())
            return self.message_queue.get(False)

    def _put_message_to_stream(self, message):
        logging.info(
            'Putting message [%s] onto stream [%s] with random partition key',
            message,
            self.stream_name
        )
        response = self.client.put_record(
            StreamName=self.stream_name,
            Data=message,
            PartitionKey=str(uuid.uuid4())
        )
        logging.info(
            'Put message [%s] onto shard [%s] of stream [%s] with sequence number [%s]',
            message,
            response['ShardId'],
            self.stream_name,
            response['SequenceNumber']
        )
