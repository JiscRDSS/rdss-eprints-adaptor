import dateutil.parser
import logging

logger = logging.getLogger(__name__)


class OAIPMHRecord(object):

    def __init__(self, rdss_cdm_remapper, record_dict, s3_objects):
        logger.info('Initialising OAIPMHRecord for %s', record_dict['identifier'])
        self.rdss_cdm_remapper = rdss_cdm_remapper
        self._record_dict = record_dict
        self._s3_objects = s3_objects

    @property
    def oai_pmh_identifier(self):
        return self._record_dict['identifier']

    @property
    def modified_date(self):
        date_string = self._record_dict['datestamp']
        return dateutil.parser.parse(date_string)

    @property
    def rdss_canonical_metadata(self):
        logger.info('Remapping record %s to canonical metadata.',
                    self.oai_pmh_identifier)
        return self.rdss_cdm_remapper.remap(self._record_dict, self._s3_objects)

    def versioned_rdss_canonical_metadata(self, previous_version_uuid):
        """ Takes an objectUuid of a previous version of this record and
            returns an appropriately versioned RDSS CDM of the record.
            """
        metadata = self.rdss_canonical_metadata
        logger.info('Versioning with %s as previous version of this record.')
        related_ids = metadata.get('objectRelatedIdentifier', [])
        related_ids.append({
            'identifier': {
                'identifierValue': previous_version_uuid,
                'identifierType': 16
            },
            'relationType': 9
        })
        metadata['objectRelatedIdentifier'] = related_ids
        return metadata
