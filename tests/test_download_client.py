import os.path
import requests_mock

from app import DownloadClient


@requests_mock.mock()
def test_download_file_success(*args):
    # Get a handle on the mocker - see https://github.com/pytest-dev/pytest/issues/2749
    requests_mocker = args[0]

    # Create the download client we'll be testing against
    download_client = DownloadClient()

    # Set up the mock response
    response_data = _get_file_bytes('tests/data/smiling.png')
    requests_mocker.get('http://eprints.test/download/file.dat', content=response_data)

    # Attempt to download the file
    file_path = download_client.download_file('http://eprints.test/download/file.dat')
    assert file_path is not None
    assert os.path.exists(file_path)

    # Read the downloaded file
    downloaded_file = _get_file_bytes(file_path)
    assert response_data == downloaded_file


@requests_mock.mock()
def test_download_file_error(*args):
    # Get a handle on the mocker - see https://github.com/pytest-dev/pytest/issues/2749
    requests_mocker = args[0]

    # Create the download client we'll be testing against
    download_client = DownloadClient()

    # Set up the mock response
    requests_mocker.get(
        'http://eprints.test/download/file.dat',
        status_code=401,
        reason='Not Found'
    )

    # Attempt to download the file
    file_path = download_client.download_file('http://eprints.test/download/file.dat')
    assert file_path is None


def _get_file_bytes(file_path):
    with open(file_path, 'rb') as file:
        return file.read()
