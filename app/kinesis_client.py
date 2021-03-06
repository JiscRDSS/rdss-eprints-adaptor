import boto3
import logging
import sys
import time
import uuid

from queue import Queue
from threading import Thread


class KinesisClient(object):

    def __init__(self, stream_name, invalid_stream_name):
        self.stream_name = stream_name
        self.invalid_stream_name = invalid_stream_name
        self.message_queue = Queue()
        self.client = self._initialise_client()
        self.queue_worker_thread = self._initialise_queue_worker()

    def _initialise_client(self):
        logging.info('Initialising Boto3 Kinesis client')
        return boto3.client('kinesis')

    def _initialise_queue_worker(self):
        try:
            # Initialise the queue worker by spawning a new thread that invokes the _process_queue
            # method.
            queue_worker = Thread(target=self._process_queue, name='KinesisQueueWorker')
            logging.info('Starting Kinesis queue worker [%s]', queue_worker)
            queue_worker.start()
            return queue_worker
        except Exception:
            logging.exception('An error occurred initialising the Kinesis queue worker')
            sys.exit(1)

    def put_message_on_queue(self, message):
        # Append the given message onto the queue.
        logging.info('Adding message [%s] to the queue', message)
        self.message_queue.put_nowait({
            'target_stream': self.stream_name,
            'message': message
        })

    def put_invalid_message_on_queue(self, message):
        # Append the given message onto the queue
        logging.info('Adding invalid message [%s] to the queue', message)
        self.message_queue.put_nowait({
            'target_stream': self.invalid_stream_name,
            'message': message
        })

    def _process_queue(self):
        # Queue processing will run a loop, forever, until the end of time, with 0.5 second
        # snoozes. This prevents the Kinesis Stream from throttling.
        while True:

            # Sleep for 0.5 seconds. A Kinesis Stream can tolerate 5 API operations per second, so
            # a 0.5 sleep should ensure consumers can also interact with it whilst we are writing
            # large quantities of messages.
            logging.debug('Sleeping for [0.5] seconds before processing next message on queue')
            time.sleep(0.5)

            # Try and grab a queue item from the queue, and check if we got something or None.
            queue_item = self._fetch_message_from_queue()
            logging.debug('Got item [%s] from queue', queue_item)
            if queue_item is not None:

                # If the worker has been poisoned, then it's time to shut down. First step: break
                # out of this loop.
                if queue_item['message'] == PoisonPill:
                    logging.info('Queue worker has been poisoned, breaking out of the loop...')
                    break
                else:
                    self._put_message_to_stream(queue_item['target_stream'], queue_item['message'])

        # Time to die.
        logging.info('All those moments will be lost in time, like tears in rain. Time to die.')

    def _fetch_message_from_queue(self):
        if self.message_queue.empty():
            logging.debug('No messages on queue to process')
            return None
        else:
            logging.info('Fetching message from queue ([%s] remaining)', self.message_queue.qsize())
            return self.message_queue.get(False)

    def _put_message_to_stream(self, target_stream, message):
        # Put the message onto the Kinesis Stream, using a random partition key. This should be
        # sufficient to guarantee random shard allocation.
        logging.info(
            'Putting message [%s] onto stream [%s] with random partition key',
            message,
            target_stream
        )
        response = self.client.put_record(
            StreamName=target_stream,
            Data=message,
            PartitionKey=str(uuid.uuid4())
        )
        logging.info(
            'Put message [%s] onto shard [%s] of stream [%s] with sequence number [%s]',
            message,
            response['ShardId'],
            target_stream,
            response['SequenceNumber']
        )


class PoisonPill:
    pass
