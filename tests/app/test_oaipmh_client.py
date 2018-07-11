from mock import patch

from app import OAIPMHClient
from dateutil import parser
from xml.dom import minidom
from urllib.parse import parse_qs

def oai_response_to_prefix(*args, **kwargs):
    """ Extracts metadataPrefix being used in call to urlopen by the underlying 
        oaipmh client and returns appropriate response. 
        """
    prefix = parse_qs(args[0].data)[b'metadataPrefix'][0]
    responses = {
            b'ore': MockResponse(_get_xml_file('tests/app/data/ore_response.xml'), 200, 'OK'),
            b'oai_dc': MockResponse(_get_xml_file('tests/app/data/oai_dc_response.xml'), 200, 'OK')
            }
    return responses[prefix]


@patch('oaipmh.client.urllib2.urlopen')
def test_fetch_records_from_with_ore(mock_urlopen):
    endpoint_url = 'http://dspace.test/dspace-oai/request' 
    oai_pmh_client = OAIPMHClient(endpoint_url, use_ore=True)
    mock_urlopen.side_effect = oai_response_to_prefix
    # Passing in a datetime, but it's not being tested
    records = oai_pmh_client.fetch_records_from(parser.parse('1970-01-01T00:00:00'))
    labels = ['one', 'two', 'three']
    for i, (label, record) in enumerate(zip(labels, records), 1):
        assert record['identifier'] == "oai:dspace.text:test_handle/{}".format(label)
        assert record['datestamp'] == parser.parse("2018-0{0}-0{0}T0{0}:0{0}:0{0}".format(i))
        dc = record['oai_dc']
        assert dc['title'][0] == 'Test Title {}'.format(label)
        assert dc['creator'][0] == 'Test Creator {}'.format(label)
        assert dc['contributor'][0] == 'Test Contributor {}'.format(label)
        assert dc['description'][0] == 'Test Description {}'.format(label)
        assert dc['identifier'][0] == 'http://hdl.handle.net/test_handle/{}'.format(label)
        assert dc['coverage'][0] == 'Test Coverage {}'.format(label)
        assert dc['date'][0] == "2018-0{0}-0{0}T0{0}:0{0}:0{0}Z".format(i)
        assert dc['date'][1] == "199{0}".format(i)

        assert dc['type'][0] == "Thesis"
        assert dc['type'][1] == "Doctoral"

        assert dc['language'][0] == "en"

        assert dc['publisher'][0] == "The University of Testing"

        assert record['file_locations'] == ["https://dspace.text/bitstream/test_handle/{0}/2/TestFile{0}.pdf".format(label)] 




@patch('oaipmh.client.urllib2.urlopen')
def test_fetch_records_from(mock_urlopen):
    # Create the EPrints client we'll be testing against
    oai_pmh_client = OAIPMHClient('http://eprints.test/cgi/oai2')
    # Get a handle on the string of the test XML
    xml_str = _get_xml_file('tests/app/data/eprints-response.xml')

    # Create a mock response to urlib2's urlopen call
    mock_urlopen.return_value = MockResponse(xml_str, 200, 'OK')

    records = oai_pmh_client.fetch_records_from(parser.parse('1970-01-01T00:00:00'))

    # Validate that we only got a single record back
    assert len(records) == 1
    record = records[0]

    # Validate the header field
    assert record['identifier'] == 'hdl:1765/1163'
    assert record['datestamp'] == parser.parse('2004-02-16T14:10:55')

    # Validate the metadata field
    assert len(record['oai_dc']) == 15
    metadata = record['oai_dc']
    assert metadata['creator'][0] == 'Pau, L-F'
    assert metadata['contributor'][0] == 'Pau, L-F'
    assert metadata['date'][0] == '2004-02-16T13:51:07Z'
    assert metadata['date'][1] == '2004-02-16T13:51:07Z'
    assert metadata['date'][2] == 'January 2004'
    assert metadata['identifier'][0] == 'http://hdl.handle.net/1765/1163'
    assert metadata['description'][0] == 'This short paper addresses the strategic challenges of ' \
                                         'deposit banks, and payment clearinghouses, posed by the' \
                                         ' growing role of mobile operators as collectors and pay' \
                                         'ment agents of flow of cash for themselves and third pa' \
                                         'rties. Through analysis and data analysis from selected' \
                                         ' operators , it is shown that mobile operators achieve ' \
                                         'as money flow handlers levels of efficiency , profitabi' \
                                         'lity ,and risk control comparable with deposit banks – ' \
                                         'Furthermore , the payment infrastructures deployed by b' \
                                         'oth are found to be quite similar , and  are analyzed i' \
                                         'n relation to  strategic challenges and opportunities T' \
                                         'his paves the way to either mobile operators taking a b' \
                                         'igger role ,or for banks to tie up such operators to th' \
                                         'em even more tightly ,or for alliances/mergers to take ' \
                                         'place ,all these options being subject to regulatory ev' \
                                         'olution as analyzed as well . The reader should acknowl' \
                                         'edge that  there is no emphasis on specific  Mobile ba' \
                                         'nking (M-Banking) technologies (security, terminals, a' \
                                         'pplication software) , nor on related market forces fr' \
                                         'om the user demand point of view.'
    assert metadata['description'][1] == 'This short paper addresses the strategic challenges of ' \
                                         'deposit banks, and payment clearinghouses, posed by the' \
                                         ' growing role of mobile operators as collectors and pay' \
                                         'ment agents of flow of cash for themselves and third pa' \
                                         'rties. Through analysis and data analysis from selected' \
                                         ' operators , it is shown that mobile operators achieve ' \
                                         'as money flow handlers levels of efficiency , profitabi' \
                                         'lity ,and risk control comparable with deposit banks – ' \
                                         'Furthermore , the payment infrastructures deployed by b' \
                                         'oth are found to be quite similar , and  are analyzed i' \
                                         'n relation to  strategic challenges and opportunities T' \
                                         'his paves the way to either mobile operators taking a b' \
                                         'igger role ,or for banks to tie up such operators to th' \
                                         'em even more tightly ,or for alliances/mergers to take ' \
                                         'place ,all these options being subject to regulatory ev' \
                                         'olution as analyzed as well . The reader should acknowl' \
                                         'edge that  there is no emphasis on specific  Mobile ba' \
                                         'nking (M-Banking) technologies (security, terminals, a' \
                                         'pplication software) , nor on related market forces fr' \
                                         'om the user demand point of view.'
    assert metadata['language'][0] == 'en_US'
    assert metadata['relation'][0] == 'ERS;ERS-2004-015-LIS'
    assert metadata['relation'][1] == 'January 2004'
    assert metadata['subject'][0] == 'mobile networks'
    assert metadata['subject'][1] == 'banking'
    assert metadata['subject'][2] == 'transaction systems'
    assert metadata['subject'][3] == 'operational cash flow'
    assert metadata['subject'][4] == 'regulations'
    assert metadata['subject'][5] == 'industry structure'
    assert metadata['subject'][6] == '5001-6182;5201-5982;HE 9713+;HD9696.B36+'
    assert metadata['title'][0] == 'Mobile operators as banks or vice-versa? and: ' \
                                   'the challenges of Mobile channels for banks'
    assert metadata['type'][0] == 'Working Paper'
    assert metadata['subject'][7] == 'M;E 44;L 96'
    assert metadata['subject'][8] == '85A;55 D;240 W;180 A'
    assert metadata['subject'][9] == '85.00;05.42;83.44'
    assert metadata['subject'][10] == 'bedrijfskunde;bedrijfseconomie;\ndraadloze communicatie;\n' \
                                      'financiële instellingen;mobiele communicatie; elektronisch' \
                                      ' betalingsverkeer'
    assert metadata['format'][0] == 'application/pdf https://ep.eur.nl/retrieve/2570/ERS 2004 015' \
                                    ' LIS.pdf'


def _get_xml_file(file_path):
    return minidom.parse(file_path).toxml()


class MockResponse(object):

    def __init__(self, response_data, status_code, status_msg):
        self.response_data = response_data
        self.status_code = status_code
        self.status_msg = status_msg
        self.headers = {'Content-Type': 'text/xml; charset=UTF-8'}

    def read(self):
        return self.response_data

    def close(self):
        pass
