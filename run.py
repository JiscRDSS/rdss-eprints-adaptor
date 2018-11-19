#!/usr/bin/env python3
import logging
import os
import sys

from processor import OAIPMHAdaptor

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s [%(threadName)s] [%(levelname)s] %(name)s - %(message)s'
)


logger = logging.getLogger(__name__)


def all_env_vars_exist(var_names):
    """ Ensure all environment variables exist and return them.
        """
    env_vars = {name: os.environ.get(name) for name in var_names}
    if not all(env_vars.values()):
        missing = (name for name, exists in env_vars.items() if not exists)
        logger.error('The following env variables have not been set: %s',
                     ', '.join(missing))
        sys.exit(2)
    return env_vars


def main():

    required_env_variables = (
        'OAI_PMH_PROVIDER',
        'OAI_PMH_ENDPOINT_URL',
        'JISC_ID',
        'ORGANISATION_NAME',
        'DYNAMODB_WATERMARK_TABLE_NAME',
        'DYNAMODB_PROCESSED_TABLE_NAME',
        'S3_BUCKET_NAME',
        'OUTPUT_KINESIS_STREAM_NAME',
        'OUTPUT_KINESIS_INVALID_STREAM_NAME',
        'RDSS_MESSAGE_API_SPECIFICATION_VERSION',
        'OAI_PMH_ADAPTOR_FLOW_LIMIT'
    )
    env_vars = all_env_vars_exist(required_env_variables)

    try:
        adaptor = OAIPMHAdaptor(
            jisc_id=env_vars['JISC_ID'],
            organisation_name=env_vars['ORGANISATION_NAME'],
            oai_pmh_endpoint_url=env_vars['OAI_PMH_ENDPOINT_URL'],
            oai_pmh_provider=env_vars['OAI_PMH_PROVIDER'],
            flow_limit=env_vars['OAI_PMH_ADAPTOR_FLOW_LIMIT'],
            message_api_version=env_vars['RDSS_MESSAGE_API_SPECIFICATION_VERSION'],
            watermark_table_name=env_vars['DYNAMODB_WATERMARK_TABLE_NAME'],
            processed_table_name=env_vars['DYNAMODB_PROCESSED_TABLE_NAME'],
            output_stream=env_vars['OUTPUT_KINESIS_STREAM_NAME'],
            invalid_stream=env_vars['OUTPUT_KINESIS_INVALID_STREAM_NAME'],
            s3_bucket_name=env_vars['S3_BUCKET_NAME']
        )
    except Exception:
        logger.exception('Cannot run the OAI-PMH Adaptor.')
        sys.exit(1)

    adaptor.run()


if __name__ == '__main__':
    main()
