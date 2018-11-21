import re
from app.rdss_cdm_remapper import RDSSCDMRemapper
from dateutil import parser

# https://github.com/JiscRDSS/rdss-message-api-specification/blob/master/schemas/types.json#L11
uuid4_regex = re.compile(
    '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
)


def test_rdss_cdm_remapper(*args):
    # Create the message generator client we'll be testing against
    rdss_cdm_remapper = RDSSCDMRemapper(12345, 'Test Organisation')

    # Generate the message using the dummy values
    test_record = _build_test_record()
    test_s3_object = _build_test_s3_objects()
    cdm_dict = rdss_cdm_remapper.remap(test_record, test_s3_object)

    # Validate the objectUuid is present and in the correct format
    assert uuid4_regex.match(cdm_dict['objectUuid'])

    # Validate the objectTitle is present and in the correct format
    assert cdm_dict['objectTitle'] == 'Test title'

    # Validate the objectPersonRole is present and in the correct format
    name_1 = cdm_dict['objectPersonRole'][0]['person']['personGivenNames']
    name_2 = cdm_dict['objectPersonRole'][1]['person']['personGivenNames']
    names = [name_1, name_2]

    assert 'Test creator' in names
    assert 'Test contributor' in names

    # Validate the objectDate is present and in the correct format
    assert parser.parse(cdm_dict['objectDate'][0]['dateValue'])

    # Validate the objectCategory is present and in the correct format
    assert cdm_dict['objectCategory'][0] == 'Test subject'

    # Validate the objectFile is present and in the correct format
    assert uuid4_regex.match(cdm_dict['objectFile'][0]['fileUuid'])
    assert cdm_dict['objectFile'][0]['fileIdentifier'] == 'download/file.dat'
    assert cdm_dict['objectFile'][0]['fileName'] == 'file.dat'
    assert cdm_dict['objectFile'][0]['fileSize'] == 17280
    assert uuid4_regex.match(
        cdm_dict['objectFile'][0]['fileChecksum'][0]['checksumUuid']
    )
    assert cdm_dict['objectFile'][0]['fileChecksum'][0][
        'checksumValue'] == '0c9a2690b41be2660db2aadad13dbf05'
    assert cdm_dict['objectFile'][0][
        'fileStorageLocation'] == 'https://rdss-prints-adaptor-test-bucket.s3.amazonaws.co' \
        'm/download/file.dat'


def _build_test_record():
    return {
        'identifier': 'test-eprints-record',
        'datestamp': '2018-03-23T12:34:56',
        'oai_dc': {
            'title': ['Test title'],
            'creator': ['Test creator'],
            'contributor': ['Test contributor'],
            'description': ['Test description'],
            'relation': ['Test relation'],
            'rights': ['Test rights'],
            'publisher': ['Test publisher'],
            'date': ['2018-03-23T09:10:15'],
            'subject': ['Test subject'],
            'identifier': ['http://eprints.test/download/file.dat']
        },
        'file_locations': [
            'http://eprints.test/download/file.dat'
        ]
    }


def _build_test_s3_objects():
    return [
        {
            'file_name': 'file.dat',
            'file_path': 'download/file.dat',
            'file_size': 17280,
            'file_checksum': '0c9a2690b41be2660db2aadad13dbf05',
            'download_url': 'https://rdss-prints-adaptor-test-bucket.s3.amazonaws.com/download/fil'
                            'e.dat'
        }
    ]
