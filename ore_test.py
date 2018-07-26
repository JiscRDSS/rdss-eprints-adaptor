from app.oai_pmh_client import OAIPMHClient
import datetime
import dateutil
import logging
import sys

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s [%(threadName)s] [%(levelname)s] %(name)s - %(message)s'
)



if __name__ == "__main__":
    dspace_url = 'https://research-repository.st-andrews.ac.uk/dspace-oai/request'
    eprints_url = 'http://eprints.lincoln.ac.uk/cgi/oai2'
    client = OAIPMHClient(eprints_url, use_ore=False)
    the_last_month = datetime.datetime.now() - datetime.timedelta(days=5)
    start = dateutil.parser.parse('2008-07-24T13:13:03.205613')
    for record in client.fetch_records_from(start):
        print(record['file_locations'])
