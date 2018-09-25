import logging

from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
from oaipmh.error import NoRecordsMatchError
from .oaiore.reader import oai_ore_reader


class OAIPMHClient(object):

    def __init__(self, url, use_ore=False):
        self.client = self._initialise_client(url)
        self.use_ore = use_ore

    def _initialise_client(self, url):
        registry = MetadataRegistry()
        registry.registerReader('oai_dc', oai_dc_reader)
        registry.registerReader('ore', oai_ore_reader)
        logging.info('Initialising OAI client with URL [%s]', url)
        return Client(url, registry)

    def fetch_records_from(self, from_datetime, until_datetime=None):
        records = self._fetch_records_by_prefix_from('oai_dc', from_datetime, until_datetime)
        if self.use_ore:
            oai_ore_records = self._fetch_records_by_prefix_from(
                'ore', from_datetime, until_datetime)
            records = self._merge_records(records, oai_ore_records)
        if not records:
            return []
        records = self._filter_empty_records(records)
        for r in records.values():
            r['file_locations'] = self._extract_file_locations(r)
        return sorted(records.values(), key=lambda k: k['datestamp'])

    def _fetch_records_by_prefix_from(self, metadata_prefix, from_datetime, until_datetime=None):
        try:
            if not until_datetime:
                logging.info('Querying for %s records from [%s]', metadata_prefix, from_datetime)
                # Fetch all records since the given from_datetime parameter.
                records = self.client.listRecords(
                    metadataPrefix=metadata_prefix, from_=from_datetime)
                logging.info('Got %s records since [%s]', metadata_prefix, from_datetime)
            else:
                logging.info('Querying for %s records from [%s] to [%s]', metadata_prefix, from_datetime, until_datetime)
                # Fetch all records between the given from_datetime and the given until_datetime
                records = self.client.listRecords(
                    metadataPrefix=metadata_prefix, from_=from_datetime, until=until_datetime)
                logging.info('Got %s records between [%s] and [%s]', metadata_prefix, from_datetime, until_datetime)

            if not records:
                return []
            else:
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

    def _filter_empty_records(self, records):
        """ Records that have been deleted will exist in the oai-pmh output, but will not have
            an `oai_dc` response. This filters them out. """
        return {k: v for k, v in records.items() if v.get('oai_dc')}

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

    def _extract_file_locations(self, record):
        file_locations = []
        if self.use_ore:
            for l in record['ore'].get('link', []):
                relation = l.get('rel', '')
                if relation == 'http://www.openarchives.org/ore/terms/aggregates':
                    file_locations.append(l.get('href', ''))
        else:
            for identifier in record['oai_dc'].get('identifier', []):
                if identifier.startswith(('http://', 'https://')):
                    file_locations.append(identifier)
        return list(filter(None, file_locations))
