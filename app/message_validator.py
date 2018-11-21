import json
import logging
import os
import jsonschema

from app.download_client import DownloadClient

logger = logging.getLogger(__name__)

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
        self.message_schema = self._get_json(self._download_message_schema())

        validator_cls = jsonschema.validators.validator_for(self.message_schema)
        self._message_validator = validator_cls(
            self.message_schema,
            resolver=jsonschema.RefResolver('', {},
                                            store={
                schema_id: self._get_json(file_path)
                for schema_id, file_path in self.model_schema_mappings
            }
            ),
            format_checker=jsonschema.FormatChecker()
        )

    def _download_model_schemas(self):
        model_schema_mappings = []
        for model_schema_document in MODEL_SCHEMA_DOCUMENTS:
            logger.info(
                'Preparing to download model JSON schema document [%s]',
                model_schema_document['file_name']
            )
            url = MODEL_SCHEMA_BASE_URL.format(
                api_version=self.api_version,
                schema_document=model_schema_document['file_name']
            )
            logger.info(
                'Got URL [%s] for model JSON schema document [%s]',
                url,
                model_schema_document['file_name']
            )
            model_schema_file = self.download_client.download_file(url)
            logger.info(
                'Got file [%s] for model JSON schema document [%s]',
                model_schema_file,
                model_schema_document['file_name']
            )
            model_schema_mappings.append((model_schema_document['schema_id'], model_schema_file))
        return model_schema_mappings

    def _download_message_schema(self):
        url = MESSAGE_SCHEMA_URL.format(api_version=self.api_version)
        logger.info('Preparing to download message JSON schema document [%s]', url)
        message_schema_file = self.download_client.download_file(url)
        logger.info(
            'Got file [%s] for message JSON schema document [%s]',
            message_schema_file,
            url
        )
        return message_schema_file

    def message_errors(self, message):
        logger.info(
            'Validating message [%s] against API specification version [%s]',
            message,
            self.api_version
        )
        error_strings = []
        # Validate the JSON payload against the JSON schema
        for error in self._message_validator.iter_errors(message):
            error_strings.append('{}: {}'.format('.'.join(
                map(str, error.path)), error.message))
        return error_strings

    def _get_json(self, file_path):
        with open(file_path) as json_data:
            return json.load(json_data)

    def shutdown(self):
        for schema_id, file_path in self.model_schema_mappings:
            try:
                os.remove(file_path)
            except Exception as e:
                logger.warning('An error occurred deleting file [%s]: %s', file_path, e)
