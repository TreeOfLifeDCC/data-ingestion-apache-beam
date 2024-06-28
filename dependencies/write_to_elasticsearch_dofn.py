# Inspired by: https://github.com/rrmerugu-archive/apache-beam-io-extras/blob/master/beam_io_extras/elasticsearch.py#L92
# and: https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/mongodbio.py#L770

"""This class writes JSON data to Elasticsearch

Example of usage::

pipeline | 'WriteToElastic' >> beam.ParDo(WriteToElasticsearchDoFn(
                host=opts.address,
                username=opts.username,
                password=opts.password,
                index=opts.index
            )
         )
     
Ensure that you have the following command line arguments set in your script to connect to Elasticsearch:
address, username, password, and index.

CLI call: python script.py --address=<address> --username=<username> --password=<password> --index='test'

Example script.py:
___________

import argparse
import apache_beam as beam
from write_to_elastic_dofn import WriteToElasticsearchDoFn

parser = argparse.ArgumentParser(description='Load DToL data from string into Elasticsearch')
parser.add_argument('--address', required=True, help='Elasticsearch URL')
parser.add_argument('--username', required=True, help='Elasticsearch username')
parser.add_argument('--password', required=True, help='Elasticsearch password')
parser.add_argument('--index', required=True, help='Elasticsearch index name')

opts = parser.parse_args()

data = [
    {
        'field1': 'string1',
        'field2': 'string2'
    }
]

with beam.Pipeline() as p:
    (p
     | 'ReadStrings' >> beam.Create(data)
     | 'WriteToElastic' >> beam.ParDo(WriteToElasticsearchDoFn(
                host=opts.address,
                username=opts.username,
                password=opts.password,
                index=opts.index
            )
         )
     )
___________

"""


import apache_beam as beam
from elasticsearch import Elasticsearch, helpers
from datetime import datetime


class WriteToElasticsearchDoFn(beam.DoFn):

    def __init__(self, host=None, username=None, password=None, index=None):
        super().__init__()
        self.host = host
        self.username = username
        self.password = password
        self.index = index

    def setup(self):
        self.es = Elasticsearch(hosts=self.host, basic_auth=(self.username, self.password))

    def start_bundle(self):
        self.actions = []

    def process(self, element):
        timestamp = datetime.now().strftime('%Y%m%d-%H%M')
        
        action = {
            "_index": self.index + "-" + timestamp,
            "_source": element
        }
        self.actions.append(action)

    def finish_bundle(self):
        if self.actions:
            helpers.bulk(self.es, self.actions)
            self.actions = []
