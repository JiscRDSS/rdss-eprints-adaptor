#!/usr/bin/env python3
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

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def main():
    settings = _get_settings()
    download_client = DownloadClient()
    dynamodb_client = DynamoDBClient(
        settings['EPRINTS_DYNAMODB_WATERMARK_TABLE_NAME'],
        settings['EPRINTS_DYNAMODB_PROCESSED_TABLE_NAME']
    )
    eprints_client = EPrintsClient(settings['EPRINTS_EPRINTS_URL'])
    kinesis_client = KinesisClient(settings['EPRINTS_OUTPUT_KINESIS_STREAM_NAME'])
    message_generator = MessageGenerator()
    s3_client = S3Client(settings['EPRINTS_S3_BUCKET_NAME'])
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
            message = _format_message(message)
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


def _format_message(message):
    try:
        json_payload = json.loads(message, strict=False)
        return json.dumps(json_payload)
    except Exception:
        logging.exception('An error occurred decoding the message [%s]', message)


def _parse_env_vars(env_var_names):
    env_vars = {name: os.environ.get(name) for name in env_var_names}
    if not all(env_vars.values()):
        missing = (name for name, exists in env_vars.items() if not exists)
        logging.error(
            'The following environment variables have not been set: [%s]',
            ', '.join(missing)
        )
        sys.exit(-1)
    return env_vars


def _get_settings():
    return _parse_env_vars((
        'EPRINTS_EPRINTS_URL',
        'EPRINTS_DYNAMODB_WATERMARK_TABLE_NAME',
        'EPRINTS_DYNAMODB_PROCESSED_TABLE_NAME',
        'EPRINTS_S3_BUCKET_NAME',
        'EPRINTS_OUTPUT_KINESIS_STREAM_NAME'
    ))


if __name__ == '__main__':
    main()
