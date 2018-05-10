import boto3

from moto import mock_s3
from app import S3Client


@mock_s3
def test_push_to_bucket():
    # Create the S3 client that we'll be testing against
    s3_client = S3Client('rdss-prints-adaptor-test-bucket')

    # Get a handle on the S3 connection and create the mock S3 bucket
    conn = boto3.resource('s3')
    conn.create_bucket(Bucket='rdss-prints-adaptor-test-bucket')

    # Push the test file to the mock S3 bucket, using the fake URL
    object_metadata = s3_client.push_to_bucket(
        'http://eprints.test/download/file.dat',
        'tests/app/data/smiling.png'
    )
    assert object_metadata is not None
    assert len(object_metadata) == 5

    # Verify the fields returned match what is expected
    assert object_metadata['file_name'] == 'file.dat'
    assert object_metadata['file_path'] == 'download/file.dat'
    assert object_metadata['file_size'] == 17280
    assert object_metadata['file_checksum'] == 'DJomkLQb4mYNsqra0T2/BQ=='
    assert object_metadata['download_url'] == 'https://rdss-prints-adaptor-test-bucket.s3.' \
                                              'amazonaws.com/download/file.dat'
