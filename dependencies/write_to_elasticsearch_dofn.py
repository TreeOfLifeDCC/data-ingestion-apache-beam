# Inspired by: https://github.com/rrmerugu-archive/apache-beam-io-extras/blob/master/beam_io_extras/elasticsearch.py#L92
# and: https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/mongodbio.py#L770

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
