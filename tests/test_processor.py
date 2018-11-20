import pytest
from processor import OAIPMHAdaptor
from .mock_helpers import mock_oai_pmh_adaptor_infra


@mock_oai_pmh_adaptor_infra()
def test_dspace_metadata_create():
    adaptor = OAIPMHAdaptor(
        jisc_id='999999',
        organisation_name='University of Test',
        oai_pmh_endpoint_url='http://dspace.test/dspace-oai/request',
        oai_pmh_provider='dspace',
        flow_limit='1',
        message_api_version='3.0.2',
        watermark_table_name='adaptor-watermark-test',
        processed_table_name='adaptor-processed-test',
        output_stream='rdss_output_stream_test',
        invalid_stream='rdss_invalid_stream_test',
        s3_bucket_name='adaptor-test'
    )
    adaptor.run()
