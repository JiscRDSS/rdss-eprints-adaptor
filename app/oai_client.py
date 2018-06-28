import logging

from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
from oaipmh.error import NoRecordsMatchError
from .oaiore.reader import oai_ore_reader 

class OAIClient(object):

    def __init__(self, url, use_ore=False):
        self.client = self._initialise_client(url)
        self.use_ore = use_ore

    def _initialise_client(self, url):
        registry = MetadataRegistry()
        registry.registerReader('oai_dc', oai_dc_reader)
        registry.registerReader('ore', oai_ore_reader)
        logging.info('Initialising OAI client with URL [%s]', url)
        return Client(url, registry)

    def fetch_records(self, from_datetime):
        records = self._fetch_records_from('oai_dc', from_datetime)
        if self.use_ore:
            oai_ore_records = self._fetch_records_from('ore', from_datetime)
            records = self._merge_records(records, oai_ore_records) 
        return sorted(records.values(), key=lambda k: k['datestamp'])

    def _fetch_records_from(self, metadata_prefix, from_datetime):
        logging.info('Querying for %s records from [%s]', metadata_prefix, from_datetime)
        try:
            # Fetch all records since the given from_datetime parameter.
            records = self.client.listRecords(metadataPrefix=metadata_prefix, from_=from_datetime)
            logging.info('Got %s records since [%s]', metadata_prefix, from_datetime)
            return dict(self._structured_record(metadata_prefix, r) for r in records)
        except NoRecordsMatchError:
            # Annoyingly, the client throws an exception if no records are found...
            logging.info('No %s records since [%s]', metadata_prefix, from_datetime)
            return []

    def _merge_records(self, records_a, records_b):
        merged_records = {}
        for k, v in records_a.items():
            merged_records[k] = {**v, **records_b[k]}
        return merged_records


    def _structured_record(self, metadata_prefix, record):
        logging.info('Converting record [%s]', record[0].identifier())
        record_dict = {
                'identifier': record[0].identifier(),
                'datestamp': record[0].datestamp(),
                metadata_prefix: self._record_metadata_to_dict(record[1])
                }
        return record[0].identifier(), record_dict

    def _record_metadata_to_dict(self, record_metadata):
        if record_metadata is not None:
            return record_metadata.getMap()
        else:
            return None
