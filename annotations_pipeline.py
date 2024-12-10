import json

import time
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import (GoogleCloudOptions, PipelineOptions, StandardOptions, WorkerOptions,
                                                  DebugOptions, SetupOptions)
from apache_beam.runners import DataflowRunner, DirectRunner


from dependencies.annotations_schema import annotations_schema
from dependencies.common_functions import parse_annotations

# Command line arguments
parser = argparse.ArgumentParser(description='Load DToL data from Json into BigQuery')
parser.add_argument('--project', required=True, help='Specify Google Cloud project')
parser.add_argument('--bq_dataset_name', required=True, help='BigQuery dataset name')
parser.add_argument('--region', required=True, help='Specify Google Cloud region')
parser.add_argument('--stagingLocation', required=True, help='Specify Cloud Storage bucket for staging')
parser.add_argument('--tempLocation', required=True, help='Specify Cloud Storage bucket for temp')
parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
parser.add_argument('--disk_size_gb', required=True, help='Disk Size of Workers')
parser.add_argument('--machine_type', required=True, help='Machine Type of Worker')

opts = parser.parse_args()

# Setting up the Beam pipeline options
options = PipelineOptions()
options.view_as(GoogleCloudOptions).project = opts.project
options.view_as(GoogleCloudOptions).region = opts.region
options.view_as(GoogleCloudOptions).staging_location = opts.stagingLocation
options.view_as(GoogleCloudOptions).temp_location = opts.tempLocation
options.view_as(WorkerOptions).max_num_workers = 64
options.view_as(WorkerOptions).disk_size_gb = int(opts.disk_size_gb)
options.view_as(WorkerOptions).machine_type = opts.machine_type
options.view_as(SetupOptions).save_main_session = True
options.view_as(SetupOptions).setup_file = '/Users/alexey/ebi_projects/data-ingestion-apache-beam/setup.py'
options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('my-pipeline-', time.time_ns())
options.view_as(StandardOptions).runner = opts.runner

# Static input and output
input = f'gs://{opts.project}/annotations*.jsonl'
bq_dataset_name = opts.bq_dataset_name

p = beam.Pipeline(options=options)

input_data = (
        p
        | "Read data from JSON file" >> beam.io.ReadFromText(input)
        | "ParseJson" >> beam.Map(lambda sample: json.loads(sample))
        | "Parse annotations file" >> beam.Map(parse_annotations)
)

input_data | "Write annotations to BigQuery" >> beam.io.WriteToBigQuery(
    table=f'{opts.project}:{bq_dataset_name}.annotations',
    schema=annotations_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
)

p.run()
