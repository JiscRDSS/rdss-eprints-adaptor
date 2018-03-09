import logging

from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
from oaipmh.error import NoRecordsMatchError


class EPrintsClient(object):

    def __init__(self, url):
        self.client = self._initialise_client(url)

    def _initialise_client(self, url):
        registry = MetadataRegistry()
        registry.registerReader('oai_dc', oai_dc_reader)
        logging.info('Initialising EPrints client with URL [%s]', url)
        return Client(url, registry)

    def fetch_records(self):
        logging.info('Querying for all EPrints records')
        records = self.client.listRecords(metadataPrefix='oai_dc')
        logging.info('Got EPrints records')
        return self._convert_eprints_records_to_dicts(records)

    def fetch_records_from(self, from_datetime):
        logging.info('Querying for EPrints records from [%s]', from_datetime)
        try:
            records = self.client.listRecords(metadataPrefix='oai_dc', from_=from_datetime)
            logging.info('Got EPrints records since [%s]', from_datetime)
            return self._convert_eprints_records_to_dicts(records)
        except NoRecordsMatchError:
            logging.info('No records in EPrints since [%s]', from_datetime)
            return []

    def _convert_eprints_records_to_dicts(self, eprints_records):
        dicts = []
        logging.info('Converting EPrints records into dicts')
        for eprints_record in eprints_records:
            logging.info('Converting EPrints record [%s]', eprints_record[0].identifier())
            dicts.append({
                'header': self._record_header_to_dict(eprints_record[0]),
                'metadata': self._record_metadata_to_dict(eprints_record[1])
            })
        logging.info('Converted [%s] EPrints records into dicts', len(dicts))
        return sorted(dicts, key=lambda k: k['header']['datestamp'])

    def _record_header_to_dict(self, record_header):
        return {
            'identifier': record_header.identifier(),
            'datestamp': record_header.datestamp()
        }

    def _record_metadata_to_dict(self, record_metadata):
        if record_metadata is not None:
            return record_metadata.getMap()
        else:
            return None
