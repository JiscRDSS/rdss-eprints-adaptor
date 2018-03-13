import logging
import socket
import uuid

from ec2_metadata import ec2_metadata
from jinja2 import select_autoescape, Environment, PackageLoader
from datetime import datetime
from dateutil import parser


class MessageGenerator(object):

    def __init__(self):
        self.env = self._initialise_environment()
        self.now = datetime.now().isoformat()

    def _initialise_environment(self):
        logging.info('Loading templates in directory [templates] from package [app]')
        # We use Jinja2 to template the messages, this block prepares the Jinja2 environment.
        return Environment(
            loader=PackageLoader('app', 'templates'),
            autoescape=select_autoescape(
                enabled_extensions=('jsontemplate'),
                default_for_string=True,
            )
        )

    def generate_metadata_create(self, record, s3_objects):
        # Generate the message by building up a dict of values and passing this into Jinja2. The
        # .jsontemplate file will be parsed and decorated with these values.
        logging.info('Fetching template [metadata_create.jsontemplate]')
        template = self.env.get_template('metadata_create.jsontemplate')
        logging.info('Rendering template using record [%s]', record)
        return template.render({
            'messageHeader': {
                'messageId': uuid.uuid4(),
                'messageTimings': {
                    'publishedTimestamp': self.now
                },
                'messageHistory': {
                    'machineAddress': self._get_machine_address(),
                    'timestamp': self.now
                }
            },
            'messageBody': {
                'objectUuid': uuid.uuid4(),
                'objectTitle': self._extract_object_title(record),
                'objectPersonRole': self._extract_object_person_roles(record),
                'objectDescription': self._extract_object_description(record),
                'objectDate': {
                    'dateValue': self._extract_object_date(record)
                },
                'objectCategory': self._extract_object_category(record),
                'objectIdentifier': {
                    'objectIdentifierValue': self._extract_object_identifier_value(record),
                },
                'objectFile': self._extract_object_files(s3_objects)
            }
        })

    def _get_machine_address(self):

        try:
            return ec2_metadata.private_ipv4
        except Exception:
            logging.exception('An error occurred retrieving EC2 metadata private ipv4 address')
            return '0.0.0.0'

    def _extract_object_title(self, record):
        if 'title' not in record['metadata'] or len(record['metadata']['title']) == 0:
            logging.warning('Record [%s] does not contain [\'metadata\'][\'title\'] field', record)
            return None
        if len(record['metadata']['title']) > 1:
            logging.warning('Field [\'metadata\'][\'title\'] has more than 1 value')
        return record['metadata']['title'][0]

    def _extract_object_person_roles(self, record):
        object_person_roles = []
        for creator in record['metadata']['creator']:
            object_person_roles.append({
                'person': {
                    'personGivenName': creator
                }
            })
        return object_person_roles

    def _extract_object_description(self, record):
        if 'description' not in record['metadata'] or len(record['metadata']['description']) == 0:
            logging.warning(
                'Record [%s] does not contain [\'metadata\'][\'description\'] field',
                record
            )
            return None
        if len(record['metadata']['description']) > 1:
            logging.warning('Field [\'metadata\'][\'description\'] has more than 1 value')
        return record['metadata']['description'][0]

    def _extract_object_date(self, record):
        if 'date' not in record['metadata'] or len(record['metadata']['date']) == 0:
            logging.warning('Record [%s] does not contain [\'metadata\'][\'date\'] field', record)
            return None
        if len(record['metadata']['date']) > 1:
            logging.warning('Field [\'metadata\'][\'date\'] has more than 1 value')
        return parser.parse(record['metadata']['date'][0]).isoformat()

    def _extract_object_category(self, record):
        object_categories = []
        for subject in record['metadata']['subject']:
            object_categories.append(subject)
        return object_categories

    def _extract_object_identifier_value(self, record):
        return record['header']['identifier']

    def _extract_object_files(self, s3_objects):
        object_files = []
        for s3_object in s3_objects:
            object_files.append({
                'fileUuid': uuid.uuid4(),
                'fileIdentifier': s3_object['file_path'],
                'fileName': s3_object['file_name'],
                'fileSize': s3_object['file_size'],
                'fileChecksum': {
                    'checksumUuid': uuid.uuid4(),
                    'checksumValue': s3_object['file_checksum']
                },
                'fileStorageLocation': s3_object['download_url']
            })
        return object_files
