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

    def fetch_records_from(self, from_datetime):
        logging.info('Querying for EPrints records from [%s]', from_datetime)
        try:
            # Fetch all records since the given from_datetime parameter.
            records = self.client.listRecords(metadataPrefix='oai_dc', from_=from_datetime)
            logging.info('Got EPrints records since [%s]', from_datetime)
            return self._convert_eprints_records_to_dicts(records)
        except NoRecordsMatchError:
            # Annoyingly, the client throws an exception if no records are found...
            logging.info('No records in EPrints since [%s]', from_datetime)
            return []

    def _convert_eprints_records_to_dicts(self, eprints_records):
        dicts = []
        # Iterate over the EPrints records, converting the record into a more usable dict format.
        logging.info('Converting EPrints records into dicts')
        for eprints_record in eprints_records:
            logging.info('Converting EPrints record [%s]', eprints_record[0].identifier())
            dicts.append({
                'header': self._record_header_to_dict(eprints_record[0]),
                'metadata': self._record_metadata_to_dict(eprints_record[1])
            })
        logging.info('Converted [%s] EPrints records into dicts', len(dicts))

        # The API doesn't return the records in any logical order. We depend on them being sorted
        # by their datestamp however, or the high watermark system won't work.
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
