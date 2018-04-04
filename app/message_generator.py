import logging
import uuid

from ec2_metadata import ec2_metadata
from jinja2 import select_autoescape, Environment, PackageLoader
from datetime import datetime
from dateutil import parser


class MessageGenerator(object):

    def __init__(self, jisc_id, organisation_name):
        self.jisc_id = jisc_id
        self.organisation_name = organisation_name
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
                'messageSequence': {
                    'sequence': uuid.uuid4()
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
                'objectRights': {
                    'rightsStatement': self._extract_object_rights(record)
                },
                'objectDate': {
                    'dateValue': self._extract_object_date(record),
                    'dateType': 6
                },
                'objectKeywords': self._extract_object_keywords(record),
                'objectCategory': self._extract_object_category(record),
                'objectIdentifier': self._extract_object_identifier_value(record),
                'objectRelatedIdentifier': self._extract_object_related_identifier(record),
                'objectOrganisationRole': self._extract_object_organisation_role(record),
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
                    'personUuid': uuid.uuid4(),
                    'personGivenName': creator,
                    'personOrganisationUnit': {
                        'organisationUnitUuid': uuid.uuid4(),
                        'organisation': {
                            'organisationJiscId': self.jisc_id,
                            'organisationName': self.organisation_name
                        }
                    }
                },
                'role': 21
            })
        for contributor in record['metadata']['contributor']:
            object_person_roles.append({
                'person': {
                    'personUuid': uuid.uuid4(),
                    'personGivenName': contributor,
                    'personOrganisationUnit': {
                        'organisationUnitUuid': uuid.uuid4(),
                        'organisation': {
                            'organisationJiscId': self.jisc_id,
                            'organisationName': self.organisation_name
                        }
                    }
                },
                'role': 21
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

    def _extract_object_rights(self, record):
        if 'rights' not in record['metadata'] or len(record['metadata']['rights']) == 0:
            logging.warning('Record [%s] does not contain [\'metadata\'][\'rights\'] field', record)
            return None
        if len(record['metadata']['rights']) > 1:
            logging.warning('Field [\'metadata\'][\'rights\'] has more than 1 value')
        return record['metadata']['rights'][0]

    def _extract_object_date(self, record):
        if 'date' not in record['metadata'] or len(record['metadata']['date']) == 0:
            logging.warning('Record [%s] does not contain [\'metadata\'][\'date\'] field', record)
            return None
        if len(record['metadata']['date']) > 1:
            logging.warning('Field [\'metadata\'][\'date\'] has more than 1 value')
        return parser.parse(record['metadata']['date'][0]).isoformat()

    def _extract_object_keywords(self, record):
        object_keywords = []
        for subject in record['metadata']['subject']:
            object_keywords.append(subject)
        return object_keywords

    def _extract_object_category(self, record):
        object_categories = []
        for subject in record['metadata']['subject']:
            object_categories.append(subject)
        return object_categories

    def _extract_object_identifier_value(self, record):
        object_identifiers = []
        for identifier in record['metadata']['identifier']:
            object_identifiers.append({
                'identifierValue': identifier,
                'identifierType': 4
            })
        return object_identifiers

    def _extract_object_related_identifier(self, record):
        object_related_identifiers = []
        for relation in record['metadata']['relation']:
            object_related_identifiers.append({
                'identifier': {
                    'identifierValue': relation,
                    'identifierType': 4,
                },
                'relationType': 13
            })
        return object_related_identifiers

    def _extract_object_organisation_role(self, record):
        object_organisation_roles = []
        for publisher in record['metadata']['publisher']:
            object_organisation_roles.append({
                'organisation': {
                    'organisationJiscId': self.jisc_id,
                    'organisationName': publisher
                },
                'role': 5
            })
        return object_organisation_roles

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
                'fileStorageLocation': s3_object['download_url'],
                'fileStoragePlatform': {
                    'storagePlatformUuid': uuid.uuid4()
                }
            })
        return object_files
