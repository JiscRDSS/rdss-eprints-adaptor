import boto3
import hashlib
import logging
import ntpath
import os
import sys
import threading

from urllib.parse import urlparse


class S3Client(object):

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.client = self._initialise_client()

    def _initialise_client(self):
        logging.info('Initialising Boto3 S3 client')
        return boto3.client('s3')

    def push_to_bucket(self, eprints_url, file_path):
        # Get a handle on the S3 object key
        object_key = self._build_object_key(eprints_url)

        # Push the file into S3. By using the upload_fileobj method, this upload will be executed
        # using multipart uplaods.
        logging.info(
            'Pushing file [%s] to S3 Bucket [%s] with key [%s]',
            file_path,
            self.bucket_name,
            object_key
        )
        file_size = os.path.getsize(file_path)
        with open(file_path, 'rb') as data:
            self.client.upload_fileobj(
                data,
                self.bucket_name,
                object_key,
                Callback=ProgressPercentage(file_size)
            )
        logging.info(
            'Finished pushing file [%s] to S3 Bucket [%s] with key [%s]',
            file_path,
            self.bucket_name,
            object_key
        )

        # Now we want to fetch some metadata about the object, specifically the size of the file.
        logging.info(
            'Fetching S3 object metadata for object [%s] in S3 Bucket [%s]',
            object_key,
            self.bucket_name
        )
        response = self.client.head_object(
            Bucket=self.bucket_name,
            Key=object_key
        )
        logging.info(
            'Got S3 object metadata for object [%s] in S3 Bucket [%s]',
            object_key,
            self.bucket_name
        )

        # Build up a dict of object metadata that is consumable by the caller of this method.
        return {
            'file_name': ntpath.basename(object_key),
            'file_path': object_key,
            'file_size': response['ContentLength'],
            'file_checksum': self._calculate_file_checksum(file_path),
            'download_url': 'https://{}.s3.amazonaws.com/{}'.format(self.bucket_name, object_key)
        }

    def _build_object_key(self, eprints_url):
        # Strip the protocol, hostname and port off of the URL, leaving just the path behind. S3
        # object keys also shouldn't start with a leading slash, so strip that too.
        eprints_path = urlparse(eprints_url).path
        if eprints_path.startswith('/'):
            return eprints_path[1:]
        return eprints_path

    def _calculate_file_checksum(self, file_path):
        # We can query the existing file on disk to calculate the checksum value.
        hash_md5 = hashlib.md5()
        logging.info('Calculating MD5 checksum for file [%s]', file_path)
        with open(file_path, 'rb') as file:
            for chunk in iter(lambda: file.read(4096), b''):
                hash_md5.update(chunk)
        checksum = hash_md5.hexdigest()
        logging.info('Got MD5 checksum value [%s] for file[%s]', checksum, file_path)
        return checksum


class ProgressPercentage(object):
    def __init__(self, object_length):
        self.object_length = object_length
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self.object_length) * 100
            sys.stdout.write(
                '\rProgress: %s / %s  (%.2f%%)\n' % (
                    self._seen_so_far,
                    self.object_length,
                    percentage
                )
            )
            sys.stdout.flush()
