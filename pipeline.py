import json

import time
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import (GoogleCloudOptions, PipelineOptions, StandardOptions, WorkerOptions,
                                                  DebugOptions, SetupOptions)
from apache_beam.runners import DataflowRunner, DirectRunner


# from table_schema import table_schema
from dependencies.samples_schema import samples_schema
from dependencies.checklist_errors_schema import checklist_errors_schema
from dependencies.symbionts_metagenomes_errors_schema import symbionts_metagenomes_errors_schema
from dependencies.data_portal_errors_schema import data_portal_errors_schema
from dependencies.common_functions import (classify_samples, process_specimens_for_data_portal, process_records_for_dwh,
                                           final_formatting)

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
options.view_as(WorkerOptions).max_num_workers = 32
options.view_as(WorkerOptions).disk_size_gb = int(opts.disk_size_gb)
options.view_as(WorkerOptions).machine_type = opts.machine_type
options.view_as(SetupOptions).save_main_session = True
options.view_as(SetupOptions).setup_file = '/home/alexey/data-ingestion-apache-beam/setup.py'
options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('my-pipeline-', time.time_ns())
options.view_as(StandardOptions).runner = opts.runner

# Static input and output
input = f'gs://{opts.project}/metadata_experiments_assemblies.jsonl'
output_path = f'gs://{opts.project}/output.txt'
bq_dataset_name = opts.bq_dataset_name


p = beam.Pipeline(options=options)

input_data = (
        p
        | "Read data from JSON file" >> beam.io.ReadFromText(input)
        | "ParseJson" >> beam.Map(lambda sample: json.loads(sample))
        | "Classify samples" >> beam.Map(classify_samples).with_outputs(
    "Errors", "Symbionts", "Metagenomes", main="Specimens")
)

errors_collection = input_data.Errors
specimens_collection = input_data.Specimens
symbionts_collection = input_data.Symbionts
metagenomes_collection = input_data.Metagenomes

data_portal_specimens_processing = (
    specimens_collection
    | "Create data portal specimens records" >> beam.Map(process_specimens_for_data_portal)
)

data_portal_specimens_processing | "Write to BigQuery" >> beam.io.WriteToBigQuery(
    table=f'{opts.project}:{bq_dataset_name}.specimens',
    schema=samples_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
)
errors_collection | "Write to Errors BigQuery" >> beam.io.WriteToBigQuery(
    table=f'{opts.project}:{bq_dataset_name}.checklist_errors',
    schema=checklist_errors_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
)

dwh_specimens_processing = (
        specimens_collection
        | "Create dwh specimens records tuple" >> beam.Map(process_records_for_dwh, "specimens").with_outputs(
    "Errors", main="Normal")
)

dwh_symbionts_processing = (
    symbionts_collection
    | "Create dwh symbionts records tuple" >> beam.Map(process_records_for_dwh, "symbionts").with_outputs(
    "Errors", main="Normal")
)

dwh_metagenomes_processing = (
    metagenomes_collection
    | "Create dwh metagenomes records tuple" >> beam.Map(process_records_for_dwh, "metagenomes").with_outputs(
    "Errors", main="Normal")
)

dwh_symbionts_processing.Errors | "Write symbionts errors to BigQuery" >> beam.io.WriteToBigQuery(
    table=f'{opts.project}:{bq_dataset_name}.symbionts_errors',
    schema=symbionts_metagenomes_errors_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
)

dwh_metagenomes_processing.Errors | "Write metagenomes errors to BigQuery" >> beam.io.WriteToBigQuery(
    table=f'{opts.project}:{bq_dataset_name}.metagenomes_errors',
    schema=symbionts_metagenomes_errors_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
)

results = (
        {'specimens': dwh_specimens_processing.Normal, 'symbionts': dwh_symbionts_processing.Normal,
         'metagenomes': dwh_metagenomes_processing.Normal}
        | beam.CoGroupByKey()
        | beam.Map(final_formatting).with_outputs()
)

results.normal | "Write to gs" >> beam.io.WriteToText(output_path)
results.error | "Write phylogenetic issues to BigQuery" >> beam.io.WriteToBigQuery(
    table=f'{opts.project}:{bq_dataset_name}.phylogenetic_errors',
    schema=data_portal_errors_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
)

p.run()
