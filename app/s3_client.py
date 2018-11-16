import boto3
import hashlib
import base64
import logging
import ntpath

from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class S3Client(object):

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.client = self._initialise_client()

    def _initialise_client(self):
        logger.info('Initialising Boto3 S3 client')
        return boto3.client('s3')

    def push_to_bucket(self, remote_url, file_path):
        # Get a handle on the S3 object key
        object_key = self._build_object_key(remote_url)

        # Push the file into S3. By using the upload_fileobj method, this upload will be executed
        # using multipart uploads.
        logger.info(
            'Pushing file [%s] to S3 Bucket [%s] with key [%s]',
            file_path,
            self.bucket_name,
            object_key
        )
        md5_checksum = self._calculate_file_checksum(file_path)
        with open(file_path, 'rb') as data:
            self.client.put_object(
                Body=data,
                Bucket=self.bucket_name,
                Key=object_key,
                ContentMD5=md5_checksum,
                Metadata={
                    'md5chksum': md5_checksum
                }
            )
        logger.info(
            'Finished pushing file [%s] to S3 Bucket [%s] with key [%s]',
            file_path,
            self.bucket_name,
            object_key
        )

        # Now we want to fetch some metadata about the object, specifically the size of the file.
        logger.info(
            'Fetching S3 object metadata for object [%s] in S3 Bucket [%s]',
            object_key,
            self.bucket_name
        )
        response = self.client.head_object(
            Bucket=self.bucket_name,
            Key=object_key
        )
        logger.info(
            'Got S3 object metadata for object [%s] in S3 Bucket [%s]',
            object_key,
            self.bucket_name
        )

        # Build up a dict of object metadata that is consumable by the caller of this method.
        return {
            'file_name': ntpath.basename(object_key),
            'file_path': object_key,
            'file_size': response['ContentLength'],
            'file_checksum': md5_checksum,
            'download_url': 's3://{}/{}'.format(self.bucket_name, object_key)
        }

    def _build_object_key(self, remote_url):
        # Strip the protocol, hostname and port off of the URL, leaving just the path behind. S3
        # object keys also shouldn't start with a leading slash, so strip that too.
        remote_path = urlparse(remote_url).path
        if remote_path.startswith('/'):
            return remote_path[1:]
        return remote_path

    def _calculate_file_checksum(self, file_path):
        # We can query the existing file on disk to calculate the checksum value.
        hash_md5 = hashlib.md5()
        logger.info('Calculating MD5 checksum for file [%s]', file_path)
        with open(file_path, 'rb') as file_in:
            for chunk in iter(lambda: file_in.read(4096), b''):
                hash_md5.update(chunk)
        checksum = base64.b64encode(hash_md5.digest()).decode('utf-8')
        logger.info('Got MD5 checksum value [%s] for file[%s]', checksum, file_path)
        return checksum
