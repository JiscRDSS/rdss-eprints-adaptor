import logging
import datetime
import dateutil
import uuid
import json
from jinja2 import select_autoescape, Environment, PackageLoader


class RDSSCDMRemapper(object):

    def __init__(self, jisc_id, organisation_name):
        self.jisc_id = jisc_id
        self.organisation_name = organisation_name
        self.env = self._initialise_environment()

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

    def _parse_datetime_with_tz(self, datetime_string):
        parsed_dt = dateutil.parser.parse(datetime_string)
        if not parsed_dt.tzinfo:
            parsed_dt = parsed_dt.replace(tzinfo=datetime.timezone.utc)
        return parsed_dt.isoformat()

    def _single_value_from_dc_metadata(self, dc_metadata, key):
        values = dc_metadata.get(key)
        if not values:
            logging.warning('DC metadata [%s] does not contain [\'%s\'] field', dc_metadata, key)
            return None
        if len(values) > 1:
            logging.warning('DC metadata [\'%s\'] has more than 1 value', key)
        return values[0]

    def _unique_value_list_from_dc_metadata(self, dc_metadata, key):
        values = dc_metadata.get(key)
        if not values:
            logging.warning('DC metadata [%s] does not contain [\'%s\'] field', dc_metadata, key)
            return []
        return list(set(values))

    def _extract_object_title(self, dc_metadata):
        return self._single_value_from_dc_metadata(dc_metadata, 'title')

    def _extract_object_description(self, dc_metadata):
        description = self._single_value_from_dc_metadata(dc_metadata, 'description')
        if not description:
            description = 'NOT FOUND'
        return description

    def _extract_object_rights(self, dc_metadata):
        rights_statement = self._single_value_from_dc_metadata(dc_metadata, 'rights')
        if not rights_statement:
            rights_statement = 'NOT FOUND'
        return rights_statement

    def _extract_object_date(self, dc_metadata):
        date_string = self._single_value_from_dc_metadata(dc_metadata, 'date')
        if not date_string:
            return None
        else:
            return self._parse_datetime_with_tz(date_string)

    def _extract_object_person_roles(self, dc_metadata):
        def _object_person_role(name, role_enum):
            return {
                'person': {
                    'personUuid': uuid.uuid4(),
                    'personGivenName': name,
                    'personOrganisationUnit': {
                        'organisationUnitUuid': uuid.uuid4(),
                        'organisation': {
                            'organisationJiscId': self.jisc_id,
                            'organisationName': self.organisation_name
                        }
                    }
                },
                'role': role_enum
            }
        people = dc_metadata.get('creator', []) + dc_metadata.get('contributor', [])
        return [_object_person_role(person, 21) for person in set(people)]

    def _extract_object_keywords(self, dc_metadata):
        return self._unique_value_list_from_dc_metadata(dc_metadata, 'subject')

    def _extract_object_category(self, dc_metadata):
        return self._unique_value_list_from_dc_metadata(dc_metadata, 'subject')

    def _doi_identifier(self, value):
        return {
            'identifierValue': value,
            'identifierType': 4
        }

    def _extract_object_identifier_value(self, dc_metadata):
        return [self._doi_identifier(_id) for _id in
                self._unique_value_list_from_dc_metadata(dc_metadata, 'identifier')]

    def _extract_object_related_identifier(self, dc_metadata):
        return [{
            'identifier': self._doi_identifier(rel),
            'relationType': 13
        } for rel in self._unique_value_list_from_dc_metadata(dc_metadata, 'relation')]

    def _extract_object_organisation_role(self, dc_metadata):
        publishers = self._unique_value_list_from_dc_metadata(dc_metadata, 'publisher')
        if not publishers:
            publishers = [self.organisation_name]
        return [{
                'organisation': {
                    'organisationJiscId': self.jisc_id,
                    'organisationName': publisher
                },
                'role': 5
                } for publisher in publishers]

    def _extract_object_files(self, s3_objects):
        return [{
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
                } for s3_object in s3_objects]

    def remap(self, record, s3_objects):
        # Generate the message by building up a dict of values and passing this into Jinja2. The
        # .jsontemplate file will be parsed and decorated with these values.
        template_name = 'rdss_cdm_3.0.0.jsontemplate'
        template = self.env.get_template(template_name)
        logging.info('Rendering template [%s] using record [%s]', template_name, record)
        dc_metadata = record['oai_dc']
        return json.loads(template.render({
            'objectUuid': uuid.uuid4(),
            'objectTitle': self._extract_object_title(dc_metadata),
            'objectPersonRole': self._extract_object_person_roles(dc_metadata),
            'objectDescription': self._extract_object_description(dc_metadata),
            'objectRights': {
                'rightsStatement': self._extract_object_rights(dc_metadata)
            },
            'objectDate': {
                'dateValue': self._extract_object_date(dc_metadata),
                'dateType': 6
            },
            'objectKeywords': self._extract_object_keywords(dc_metadata),
            'objectCategory': self._extract_object_category(dc_metadata),
            'objectIdentifier': self._extract_object_identifier_value(dc_metadata),
            'objectRelatedIdentifier': self._extract_object_related_identifier(dc_metadata),
            'objectOrganisationRole': self._extract_object_organisation_role(dc_metadata),
            'objectFile': self._extract_object_files(s3_objects)
        }))
