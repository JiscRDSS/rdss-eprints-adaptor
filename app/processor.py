import datetime
import dateutil
import os
import itertools
import json
import logging

from app.oai_pmh_client import OAIPMHClient
from app.oai_pmh_record import OAIPMHRecord
from app.state_storage import AdaptorStateStore, RecordState
from app.kinesis_client import KinesisClient, PoisonPill
from app.download_client import DownloadClient
from app.s3_client import S3Client
from app.rdss_cdm_remapper import RDSSCDMRemapper
from app.message_validator import MessageValidator
from app.messages import MetadataCreate, MetadataUpdate

logger = logging.getLogger(__name__)


class OAIPMHAdaptor(object):

    USE_ORE = {
        'dspace': True,
        'eprints': False
    }

    def __init__(self,
                 jisc_id,
                 organisation_name,
                 oai_pmh_endpoint_url,
                 oai_pmh_provider,
                 flow_limit,
                 message_api_version,
                 watermark_table_name,
                 processed_table_name,
                 output_stream,
                 invalid_stream,
                 s3_bucket_name
                 ):
        logger.info('Initialising the OAIPMHAdaptor as a(n) %s adaptor.',
                    oai_pmh_provider)
        self.state_store = AdaptorStateStore(
            watermark_table_name,
            processed_table_name
        )
        self.oai_pmh_client = OAIPMHClient(
            oai_pmh_endpoint_url,
            self.USE_ORE[oai_pmh_provider]
        )
        self.download_client = DownloadClient()
        self.kinesis_client = KinesisClient(
            output_stream,
            invalid_stream
        )
        self.s3_client = S3Client(s3_bucket_name)
        self.rdss_cdm_remapper = RDSSCDMRemapper(
            jisc_id,
            organisation_name
        )
        self.message_validator = MessageValidator(
            message_api_version
        )
        self.flow_limit = int(flow_limit)
        self.oai_pmh_provider = oai_pmh_provider

    def _get_latest_datetime(self):
        """ Returns the latest datetime from the state store, or
            sets a sane start datetime if none present (e.g. on first
            run of the adaptor) """
        latest_datetime = self.state_store.get_high_watermark()
        if not latest_datetime:
            latest_datetime = datetime.datetime(2000, 1, 1, 0, 0)
            self.state_store.update_high_watermark(latest_datetime.isoformat())
        return latest_datetime

    def _record_not_already_processed(self, record):
        """ Determines whether this particular manifestation of the record has
            already been processed by comparing the update datetimes of record
            returned from the OAI-PMH API and the stored RecordState.
            Necessary as the EPrints OAI-PMH `from` and `until` queries doesn't
            have a granularity of seconds (or less).
            """
        last_updated = self.state_store.get_record_last_updated(record['identifier'])
        return not record['datestamp'] == last_updated

    def _poll_for_changed_records(self):
        """ Queries the OAI-PMH endpoint for a limited number of changed records,
            determined by the flow_limit set for the adaptor. As the number of desired
            responses from an OAI-PMH endpoint cannot be provided as an argument, this
            attempts to limit the number of calls to the endpoint by stepping over 24
            hour spans of time until records are found.
            """
        start_timestamp = self._get_latest_datetime()
        today = datetime.datetime.today()
        records = []
        while not records:
            if start_timestamp.date() == today.date():
                logger.info('Start timestamp %s is today %s', start_timestamp.date(), today.date())
                records = list(itertools.islice(
                    filter(self._record_not_already_processed,
                           self.oai_pmh_client.fetch_records_from(start_timestamp)
                           ),
                    self.flow_limit))
                break
            else:
                until_timestamp = start_timestamp + datetime.timedelta(days=1)
                records = list(itertools.islice(
                    filter(self._record_not_already_processed,
                           self.oai_pmh_client.fetch_records_from(start_timestamp, until_timestamp)
                           ),
                    self.flow_limit))
                start_timestamp = until_timestamp
        return records

    def _push_files_to_s3(self, record):
        """ Download all files referenced in the record from the OAI-PMH endpoint
            and push these files to the S3 bucket for the adaptor.
            """
        s3_file_locations = []
        for file_location in record['file_locations']:
            file_path = self.download_client.download_file(file_location)
            if file_path is not None:
                s3_file_locations.append(
                    self.s3_client.push_to_bucket(file_location, file_path)
                )
                try:
                    os.remove(file_path)
                except FileNotFoundError:
                    logger.warning('An error occurred removing file [%s]', file_path)
            else:
                logger.warning('Unable to download file [%s], skipping file', file_location)
        return s3_file_locations

    def _process_record(self, record):
        """ Undertakes the processing of a single record, converting the
            OAI-PMH output to the RDSS CDM, uploading the files associated with
            the record to an s3 bucket, creating a `MetadataCreate` or `MetadataUpdate`
            message, and placing that message on the relevant Kinesis Stream.

            The `try - except` and subsequent error state storage has been included
            almost exclusively to handle an occasional error with file download. This
            could probably be handled in a more robust way.
            """
        logger.info('Processing record [%s]', record['identifier'])
        try:
            s3_objects = self._push_files_to_s3(record)
            oai_pmh_record = OAIPMHRecord(self.rdss_cdm_remapper, record, s3_objects)

            record_state = RecordState.create_from_record(oai_pmh_record)
            prev_record_state = self.state_store.get_record_state(
                oai_pmh_record.oai_pmh_identifier)

            if not prev_record_state.message_body or not prev_record_state.successful_create:
                message_creator = MetadataCreate(self.message_validator, self.oai_pmh_provider)
                message = message_creator.generate(
                    oai_pmh_record.rdss_canonical_metadata
                )

            elif record_state != prev_record_state:
                message_creator = MetadataUpdate(self.message_validator, self.oai_pmh_provider)
                message = message_creator.generate(
                    oai_pmh_record.versioned_rdss_canonical_metadata(
                        prev_record_state.object_uuid
                    )
                )
            else:
                # At present this won't occur due to the generation of UUID's for
                # each new message.
                logger.info(
                    'Skipping %s as no change in RDSS CDM manifestation of record.',
                    oai_pmh_record.oai_pmh_identifier)

            if message.is_valid:
                self.kinesis_client.put_message_on_queue(json.dumps(message.as_json))
            else:
                self.kinesis_client.put_invalid_message_on_queue(json.dumps(message.as_json))

            record_state.update_with_message(message)
            self._update_adaptor_state(record_state)
        except Exception as e:
            logger.error('Processing record %s raised error %s', record['identifier'], e)
            record_state = RecordState.create_from_error(
                record['identifier'],
                record['datestamp'],
                str(e)
            )
            self._update_adaptor_state(record_state)
            raise

    def _update_adaptor_state(self, latest_record_state):
        """ Updates the record in the processed state store, and updates
            the high watermark to be the modified date of this record with one
            second added - oai-pmh API's don't seem to respond to a timedelta
            smaller than this.
            :latest_record_state: RecordState
            """
        new_high_watermark = dateutil.parser.parse(
            latest_record_state.last_updated) + datetime.timedelta(seconds=1)
        self.state_store.put_record_state(latest_record_state)
        self.state_store.update_high_watermark(new_high_watermark.isoformat())

    def _shutdown(self):
        logger.info('Shutting adaptor down...')
        if self.kinesis_client is not None:
            self.kinesis_client.put_message_on_queue(PoisonPill)
        if self.message_validator is not None:
            self.message_validator.shutdown()

    def run(self):
        for record in self._poll_for_changed_records():
            logger.info('Processing record [%s]', record)
            self._process_record(record)

        self._shutdown()
