import argparse
import json
import logging
import os
import sys

from app import EPrintsClient
from app import DownloadClient
from app import DynamoDBClient
from app import KinesisClient
from app import MessageGenerator
from app import S3Client
from datetime import datetime
from dateutil import parser

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def main():
    args = _parse_args()

    download_client = DownloadClient()
    dynamodb_client = DynamoDBClient(
        args.dynamodb_watermark_table_name,
        args.dynamodb_processed_table_name
    )
    eprints_client = EPrintsClient(args.eprints_url)
    kinesis_client = KinesisClient(args.output_kinsis_stream_name)
    message_generator = MessageGenerator()
    s3_client = S3Client(args.s3_bucket_name)

    start_timestamp = args.start_timestamp
    if start_timestamp is not None:
        start_timestamp = parser.parse(start_timestamp)
    else:
        start_timestamp = dynamodb_client.fetch_high_watermark()
        if start_timestamp is None:
            start_timestamp = datetime.utcfromtimestamp(0)

    records = eprints_client.fetch_records_from(start_timestamp)
    for record in records:
        logging.info('Processing EPrints record [%s]', record)
        _process_record(
            download_client,
            dynamodb_client,
            s3_client,
            kinesis_client,
            message_generator,
            record
        )


def _process_record(download_client, dynamodb_client, s3_client, kinesis_client, message_generator,
                    record):
    eprints_identifier = record['header']['identifier']
    status = dynamodb_client.fetch_processed_status(eprints_identifier)
    logging.info(
        'Got processed status [%s] for EPrints identifier [%s]',
        status,
        eprints_identifier
    )
    if status == 'Success':
        logging.info(
            'EPrints record [%s] already successfully processed, skipping',
            eprints_identifier
        )
        return
    else:
        logging.info('Processing EPrints record [%s]', eprints_identifier)
        message, status, reason = None, 'Success', '-'
        try:
            s3_objects = _push_files_to_s3(download_client, s3_client, record)
            message = message_generator.generate_metadata_create(record, s3_objects)
            message = format_message(message)
            kinesis_client.put_message_on_queue(message)
            dynamodb_client.update_high_watermark(record['header']['datestamp'])
        except Exception as e:
            logging.exception('An error occurred processing EPrints record [%s]', record)
            status, reason = 'Failure', str(e)
        dynamodb_client.update_processed_record(
            record['header']['identifier'],
            message if message is not None and len(message) > 0 else '-',
            status,
            reason
        )


def _push_files_to_s3(download_client, s3_client, record):
    file_locations = []
    identifiers = record['metadata']['identifier']
    for identifier in identifiers:
        if identifier.startswith('http://'):
            file_path = download_client.download_file(identifier)
            if file_path is not None:
                file_locations.append(
                    s3_client.push_to_bucket(identifier, file_path)
                )
                os.remove(file_path)
            else:
                logging.warning('Unable to download EPrints file [%s], skipping file', identifier)
    return file_locations


def format_message(message):
    try:
        json_payload = json.loads(message, strict=False)
        return json.dumps(json_payload)
    except Exception:
        logging.exception('An error occurred decoding the message [%s]', message)


def _parse_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        'eprints_url',
        help='URL of the EPrints OAI-PMH endpoint'
    )
    arg_parser.add_argument(
        'dynamodb_watermark_table_name',
        help='Name of the DynamoDB table containing the high watermark'
    )
    arg_parser.add_argument(
        'dynamodb_processed_table_name',
        help='Name of the DynamoDB table containing the processed EPrints identifiers'
    )
    arg_parser.add_argument(
        's3_bucket_name',
        help='Name of the S3 Bucket into which EPrints files are stored'
    )
    arg_parser.add_argument(
        'output_kinsis_stream_name',
        help='Name of the Kinsis Stream to put processed messages on'
    )
    arg_parser.add_argument(
        '--start_timestamp',
        help='ISO86001 timestamp to start processing EPrints records from'
    )
    return arg_parser.parse_args()


if __name__ == '__main__':
    main()
