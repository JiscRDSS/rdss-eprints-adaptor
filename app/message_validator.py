import json
import logging
import os

from app import DownloadClient
from jsonschema import validate, FormatChecker, RefResolver

MODEL_SCHEMA_BASE_URL = 'https://raw.githubusercontent.com/JiscRDSS/rdss-message-api-specificatio' \
                        'n/{api_version}/schemas/{schema_document}'
MODEL_SCHEMA_DOCUMENTS = [
    {
        'file_name': 'enumeration.json',
        'schema_id': 'https://www.jisc.ac.uk/rdss/schema/enumeration.json/#'
    },
    {
        'file_name': 'header.json',
        'schema_id': 'https://www.jisc.ac.uk/rdss/schema/header.json/#'
    },
    {
        'file_name': 'intellectual_asset.json',
        'schema_id': 'https://www.jisc.ac.uk/rdss/schema/intellectual_asset.json/#'
    },
    {
        'file_name': 'material_asset.json',
        'schema_id': 'https://www.jisc.ac.uk/rdss/schema/material_asset.json/#'
    },
    {
        'file_name': 'research_object.json',
        'schema_id': 'https://www.jisc.ac.uk/rdss/schema/research_object.json/#'
    },
    {
        'file_name': 'types.json',
        'schema_id': 'https://www.jisc.ac.uk/rdss/schema/types.json/#'
    }
]
MESSAGE_SCHEMA_URL = 'https://raw.githubusercontent.com/JiscRDSS/rdss-message-api-specification/{' \
                     'api_version}/messages/message_schema.json'


class MessageValidator(object):

    def __init__(self, api_version):
        self.api_version = api_version
        self.download_client = DownloadClient()
        self.model_schema_mappings = self._download_model_schemas()
        self.message_schema_file_path = self._download_message_schema()

    def _download_model_schemas(self):
        model_schema_mappings = []
        for model_schema_document in MODEL_SCHEMA_DOCUMENTS:
            logging.info(
                'Preparing to download model JSON schema document [%s]',
                model_schema_document['file_name']
            )
            url = MODEL_SCHEMA_BASE_URL.format(
                api_version=self.api_version,
                schema_document=model_schema_document['file_name']
            )
            logging.info(
                'Got URL [%s] for model JSON schema document [%s]',
                url,
                model_schema_document['file_name']
            )
            model_schema_file = self.download_client.download_file(url)
            logging.info(
                'Got file [%s] for model JSON schema document [%s]',
                model_schema_file,
                model_schema_document['file_name']
            )
            model_schema_mappings.append((model_schema_document['schema_id'], model_schema_file))
        return model_schema_mappings

    def _download_message_schema(self):
        url = MESSAGE_SCHEMA_URL.format(api_version=self.api_version)
        logging.info('Preparing to download message JSON schema document [%s]', url)
        message_schema_file = self.download_client.download_file(url)
        logging.info(
            'Got file [%s] for message JSON schema document [%s]',
            message_schema_file,
            url
        )
        return message_schema_file

    def validate_message(self, message):
        logging.info(
            'Validating message [%s] against API specification version [%s]',
            message,
            self.api_version
        )

        # Validate the JSON payload against the JSON schema
        validate(
            json.loads(message),
            self._get_json(self.message_schema_file_path),
            resolver=RefResolver(
                '',
                {},
                store={
                    schema_id: self._get_json(file_path)
                    for schema_id, file_path in self.model_schema_mappings
                }
            ),
            format_checker=FormatChecker()
        )

    def _get_json(self, file_path):
        with open(file_path) as json_data:
            return json.load(json_data)

    def shutdown(self):
        for schema_id, file_path in self.model_schema_mappings:
            try:
                os.remove(file_path)
            except Exception as e:
                logging.warning('An error occurred deleting file [%s]: %s', file_path, e)
