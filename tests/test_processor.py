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
            output_stream=env_vars['OUTPUT_KINESIS_STREAM_NAME'],
            invalid_stream=env_vars['OUTPUT_KINESIS_INVALID_STREAM_NAME'],
            s3_bucket_name=env_vars['S3_BUCKET_NAME']
            )
    adaptor.run()
