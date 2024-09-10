import argparse
import gzip

import apache_beam as beam
import json
import os
import pendulum
import requests
import time

from airflow.decorators import dag, task
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from apache_beam.options.pipeline_options import PipelineOptions
from bs4 import BeautifulSoup
from lxml import etree
from google.cloud import storage
from urllib.request import urlretrieve

# # Command line arguments
# parser = argparse.ArgumentParser(description='ETL DAG for annotation data: from Ensembl into BigQuery')
# parser.add_argument('--project', required=True, help='Specify Google Cloud project')
# parser.add_argument('--bq_dataset_name', required=True, help='BigQuery dataset name')
# parser.add_argument('--region', required=True, help='Specify Google Cloud region')
# parser.add_argument('--stagingLocation', required=True, help='Specify Cloud Storage bucket for staging')
# parser.add_argument('--tempLocation', required=True, help='Specify Cloud Storage bucket for temp')
# parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
# parser.add_argument('--disk_size_gb', required=True, help='Disk Size of Workers')
# parser.add_argument('--machine_type', required=True, help='Machine Type of Worker')
# parser.add_argument('--max_num_workers', required=True, help='Maximum number of workers')
# parser.add_argument('--save_main_session', required=True, help='Save main session')
# parser.add_argument('--setup_file', required=True, help='Setup file')
# parser.add_argument('--annotations_url', required=True, help="Website with links for annotation data: "
#                                                              "e.g. 'https://projects.ensembl.org/darwin-tree-of-life/'")
# parser.add_argument('--annotation_json_file', required=True, help='Name of json file to store annotation links')
# parser.add_argument('--taxonomy_jsonl_file', required=True, help='Name of jsonl file to store taxonomy data')
# parser.add_argument('--apache_beam_script', required=True, help='Name of python script to run on Apache Beam')
# opts = parser.parse_args()

GCP_PROJECT = os.environ.get('GCP_PROJECT', 'prj-ext-prod-planet-bio-dr')
GCP_BQ_DATASET = os.environ.get('GCP_BQ_DATASET', 'annotations')
GCP_STAGING_LOCATION = os.environ.get('GCP_STAGING_LOCATION')
GCP_TEMP_LOCATION = os.environ.get('GCP_TEMP_LOCATION')
GCP_RUNNER = os.environ.get('GCP_RUNNER')
GCP_DISK_SIZE = os.environ.get('GCP_DISK_SIZE')
GCP_MACHINE_TYPE = os.environ.get('GCP_MACHINE_TYPE')
GCP_MAX_WORKERS = os.environ.get('GCP_MAX_WORKERS')
GCP_SAVE_MAIN_SESSION = os.environ.get('GCP_SAVE_MAIN_SESSION')
SETUP_FILE = os.environ.get('SETUP_FILE')
ANNOTATIONS_URL = os.environ.get('ANNOTATIONS_URL', 'https://projects.ensembl.org/darwin-tree-of-life/')
ANNOTATION_JSON = os.environ.get(
    'ANNOTATION_JSON',
    '/Users/juann/PycharmProjects/annotationsDataIngestion/annotations_to_import.json'
)
APACHE_BEAM_SCRIPT = os.environ.get(
    'APACHE_BEAM_SCRIPT',
    '/Users/juann/PycharmProjects/annotationsDataIngestion/mock_beam_pipeline.py'
)

@dag(schedule=None,
     start_date=pendulum.datetime(2024, 8, 1, tz="UTC"))
def annotations_pipeline_dag():
    @task()
    def parse_annotations(url, json_file_name):
        print(f'Parsing annotations urls from {url}')

        # Create an HTTP GET request
        annotations = requests.get(url)

        # Raise an exception if we made a request resulting in an error
        annotations.raise_for_status()

        # Access the content of the response in Unicode
        annotations_text = annotations.text

        # Use BeautifulSoup to parse the HTML
        soup_archive = BeautifulSoup(annotations_text, 'html.parser')
        rows = soup_archive.find_all("tr")

        # Saving JSON as an object in GCS
        client = storage.Client(project=GCP_PROJECT)
        bucket = client.get_bucket(GCP_PROJECT)
        blob = bucket.blob(json_file_name)
        print(f'{blob.name} storage object created.')

        with blob.open("w") as f:
            for row in rows:
                try:
                    cells = row.find_all("td")
                    links = cells[4].find_all("a")
                    link = links[0].get("href").strip()
                    accession = cells[2].text.strip()
                    record = dict()
                    record['accession'] = accession
                    record['link'] = link
                    f.write(f"{json.dumps(record)}\n")
                except IndexError:
                    continue

        print(f'Annotations urls saved to {blob.name}')

        return blob.name

    @task()
    def annotations_to_cloud_storage(annotations_url_blob_name):
        client = storage.Client(project=GCP_PROJECT)
        bucket = client.get_bucket(GCP_PROJECT)
        blob_annotation_urls = bucket.blob(annotations_url_blob_name)
        print(f'Ingesting annotation data from urls in blob {blob_annotation_urls.name}')

        with blob_annotation_urls.open(mode='r') as f:
            for i, line in enumerate(f):
                print(f"Working on: {i}")
                data = json.loads(line.rstrip())
                accession, link = data['accession'], data['link']
                filename = link.split("/")[-1]
                urlretrieve(link, filename)
                client = storage.Client(project=GCP_PROJECT)
                bucket = client.get_bucket(GCP_PROJECT)
                blob = bucket.blob(f'annotations_{accession}.jsonl')

                with blob.open(mode='w') as output:
                    with gzip.open(filename, 'rb') as f:
                        for _ in range(5):
                            next(f)
                        for line in f:
                            record = dict()
                            data = line.rstrip().decode("utf-8").split("\t")
                            record["record_type"] = data[2]
                            record["info"] = data[-1]
                            record["accession"] = accession
                            output.write(f"{json.dumps(record)}\n")

        print(f'Annotation data ingestion from {blob_annotation_urls.name} finished.')

    @task
    def build_phylogeny_table(annotations_url_blob_name):
        print(f'Building the phylogeny table for {annotations_url_blob_name}')
        client = storage.Client(project=GCP_PROJECT)
        bucket = client.get_bucket(GCP_PROJECT)
        blob_taxonomy = bucket.blob(
            f'test_{annotations_url_blob_name[-len(annotations_url_blob_name):-5]}_taxonomy.jsonl')
        print(f'{blob_taxonomy.name} storage object created.')

        with blob_taxonomy.open(mode='w') as bt:
            client = storage.Client(project=GCP_PROJECT)
            bucket = client.get_bucket(GCP_PROJECT)
            blob_annotation_urls = bucket.blob(annotations_url_blob_name)

            with blob_annotation_urls.open(mode='r') as f:
                for i, line in enumerate(f):
                    print(f"Working on: {i}")
                    sample_to_return = dict()
                    data = json.loads(line.rstrip())
                    sample_to_return["accession"] = data["accession"]

                    response = requests.get(
                        f"https://www.ebi.ac.uk/ena/browser/api/xml/{sample_to_return['accession']}")
                    root = etree.fromstring(response.content)
                    sample_to_return['tax_id'] = root.find("ASSEMBLY").find("TAXON").find("TAXON_ID").text

                    phylogenetic_ranks = ('kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species')

                    for rank in phylogenetic_ranks:
                        sample_to_return[rank] = None
                    response = requests.get(f"https://www.ebi.ac.uk/ena/browser/api/xml/{sample_to_return['tax_id']}")
                    root = etree.fromstring(response.content)
                    try:
                        for taxon in root.find('taxon').find('lineage').findall('taxon'):
                            rank = taxon.get('rank')
                            if rank in phylogenetic_ranks:
                                scientific_name = taxon.get('scientificName')
                                sample_to_return[rank] = scientific_name if scientific_name else None
                    except AttributeError:
                        pass
                    bt.write(f"{json.dumps(sample_to_return)}\n")

        print(f'Phylogeny table saved to {blob_taxonomy.name}')

    run_beam_pipeline = BeamRunPythonPipelineOperator(
        task_id="GCS_to_BigQuery_Beam_pipeline",
        py_file=APACHE_BEAM_SCRIPT,
        runner=GCP_RUNNER,  # "DirectRunner",  # "DataflowRunner"
        py_options=[],
        pipeline_options={
            'stagingLocation': GCP_STAGING_LOCATION,
            'tempLocation': GCP_TEMP_LOCATION,
            'bq_dataset_name': GCP_BQ_DATASET,
            'disk_size_gb': int(GCP_DISK_SIZE),
            'machine_type': GCP_MACHINE_TYPE,
            'max_num_workers': GCP_MAX_WORKERS,
            'save_main_session': GCP_SAVE_MAIN_SESSION,
            'setup_file': SETUP_FILE
        },
        py_requirements=["apache-beam[gcp]==2.26.0"],
        py_interpreter="python3",
        py_system_site_packages=False,
        dataflow_config=DataflowConfiguration(
            job_name='{0}{1}'.format('my-pipeline-', time.time_ns()),
            project_id=GCP_PROJECT,
            location='europe-west2'
        )
    )

    # Defining dependencies between airflow tasks
    blob_annotations_name = parse_annotations(url=ANNOTATIONS_URL, json_file_name=ANNOTATION_JSON)

    [
        annotations_to_cloud_storage(annotations_url_blob_name=blob_annotations_name),
        build_phylogeny_table(annotations_url_blob_name=blob_annotations_name)
    ] >> run_beam_pipeline


annotation_dag = annotations_pipeline_dag()
