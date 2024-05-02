import json

import time
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import (GoogleCloudOptions, PipelineOptions, StandardOptions, WorkerOptions,
                                                  DebugOptions, SetupOptions)
from apache_beam.runners import DataflowRunner, DirectRunner


from table_schema import table_schema
from samples_schema import samples_schema
from checklist_errors_schema import checklist_errors_schema
from common_functions import (classify_samples, process_specimens_for_dwh, process_specimens_for_data_portal)

# Command line arguments
parser = argparse.ArgumentParser(description='Load DToL data from Json into BigQuery')
parser.add_argument('--project', required=True, help='Specify Google Cloud project')
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
options.view_as(SetupOptions).setup_file = 'setup.py'
options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('my-pipeline-', time.time_ns())
options.view_as(StandardOptions).runner = opts.runner

# Static input and output
input = f'gs://{opts.project}/dtol_data.jsonl'


p = beam.Pipeline(options=options)

input_data = (
        p
        | "Read data from JSON file" >> beam.io.ReadFromText(input)
        | "ParseJson" >> beam.Map(lambda sample: json.loads(sample))
        | "Classify samples" >> beam.ParDo(classify_samples).with_outputs(
    "Errors", "Symbionts", "Metagenomes", main="Specimens")
)

errors_collection = input_data.Errors
specimens_collection = input_data.Specimens
symbionts_collection = input_data.Symbionts
metagenomes_collection = input_data.Metagenomes

# dwh_specimens_processing = (
#         specimens_collection
#         | "Create specimens tuple" >> beam.Map(process_specimens_for_dwh)
# )

data_portal_specimens_processing = (
    specimens_collection
    | "Create data portal specimens record" >> beam.Map(process_specimens_for_data_portal)
)

data_portal_specimens_processing | "Write to BigQuery" >> beam.io.WriteToBigQuery(
    f'{opts.project}:dtol.specimens',
    schema=samples_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
)
errors_collection | "Write to Errors BigQuery" >> beam.io.WriteToBigQuery(
    f'{opts.project}:dtol.checklist_errors',
    schema=checklist_errors_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
)

# symbionts_processing = (
#         symbionts_collection
#         | "Create symbionts tuple" >> beam.Map(process_symbiont).with_outputs(
#     "DWHSymbionts", main="DataPortalSymbionts")
# )
#
# metagenomes_processing = (
#         metagenomes_collection
#         | "Create metagenomes tuple" >> beam.Map(process_metagenomes).with_outputs(
#     "DWHMetagenomes", main="DataPortalMetagenomes")
)

# organisms_raw_data = (
#         organisms_collection
#         | "Get raw data for organisms" >> beam.Map(get_reads)
# )
#
# specimens_raw_data = (
#         specimens_collection
#         | "Get raw data for specimens" >> beam.Map(get_reads)
# )

# organisms_assemblies = (
#         organisms_collection
#         | "Get assemblies for organisms" >> beam.Map(parse_assemblies)
# )
#
# specimens_assemblies = (
#         specimens_collection
#         | "Get assemblies for specimens" >> beam.Map(parse_assemblies)
# )

# raw_data = (
#         (organisms_raw_data, specimens_raw_data)
#         | "Flatten organisms and specimens raw data" >> beam.Flatten()
#         | "Group organisms and specimens raw data" >> beam.GroupByKey()
#         | "Merge records into one list" >> beam.Map(merge_data_records)
# )
#
# assemblies = (
#         (organisms_assemblies, specimens_assemblies)
#         | "Flatten organisms and specimens assemblies" >> beam.Flatten()
#         | "Group organisms and specimens assemblies" >> beam.GroupByKey()
#         | "Merge assemblies into one list" >> beam.Map(merge_data_records)
# )
#
# symbionts_raw_data = (
#         symbionts_processing
#         | "Get raw data for symbionts" >> beam.Map(get_reads)
# )
#
# symbionts_assemblies = (
#         symbionts_processing
#         | "Get assemblies for symbionts" >> beam.Map(parse_assemblies)
# )

# metagenomes_raw_data = (
#         metagenomes_processing
#         | "Get raw data for metagenomes" >> beam.Map(get_reads)
# )
#
# metagenomes_assemblies = (
#         metagenomes_processing
#         | "Get assemblies for metagenomes" >> beam.Map(parse_assemblies)
# )

# results = (
#         {'specimens': specimens_processing,
#          'symbionts': symbionts_processing, 'metagenomes': metagenomes_processing,
#          'raw_data': raw_data, 'assemblies': assemblies,
#          'symbionts_raw_data': symbionts_raw_data,
#          'symbionts_assemblies': symbionts_assemblies,
#          'metagenomes_raw_data': metagenomes_raw_data,
#          'metagenomes_assemblies': metagenomes_assemblies}
#         | beam.CoGroupByKey()
#         | "Final formatting" >> beam.Map(final_formatting).with_outputs()
# )
#
# normal_results = results.normal
# error_results = results.error

# normal_results | "Write to BigQuery" >> beam.io.WriteToBigQuery(
#     'prj-ext-prod-dtol-gcp-dr:dtol.dwh',
#     schema=table_schema,
#     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
#     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
# )

# error_results | "Write errors to BigQuery" >> beam.io.WriteToBigQuery(
#     'prj-ext-prod-dtol-gcp-dr:dtol.dwh_errors',
#     schema=error_table_schema,
#     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
#     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
# )

p.run()
