"""Script to create jsonl data for ingestion"""

import argparse
import json
from collections import defaultdict
from typing import Any

import requests

from dependencies.samples_schema import samples_schema

EXPERIMENTS: defaultdict[Any, list] = defaultdict(list)
ASSEMBLIES: defaultdict[Any, list] = defaultdict(list)
ANALYSES: defaultdict[Any, list] = defaultdict(list)
SAMPLES = {}

ENA_ROOT_URL = "https://www.ebi.ac.uk/ena/portal/api/filereport"
BIOSAMPLES_ROOT_URL = "https://www.ebi.ac.uk/biosamples/samples"

parser = argparse.ArgumentParser(
    description="Collect metadata, experiments and assemblies"
)
parser.add_argument(
    "--study_id",
    required=True,
    help="Specify Project ENA study id",
)
parser.add_argument(
    "--project_tag",
    required=True,
    help="Specify Project tag for BioSamples search",
)

parser.add_argument(
    "--project_name",
    required=True,
    help="Specify Project Name for Data Portal",
)
opts = parser.parse_args()

STUDY_ID = opts.study_id
PROJECT_TAG = opts.project_tag
PROJECT_NAME = opts.project_name


def main():
    """
    Collect DToL metadata from BioSamples, experiments and assemblies
    from the ENA
    """
    print(f"Working on {PROJECT_NAME}")
    # collect required experiments and assemblies fields
    experiment_fields = []
    assemblies_fields = []
    analysis_fields = []
    for field in samples_schema["fields"]:
        if field["name"] == "experiments":
            for experiment_field in field["fields"]:
                experiment_fields.append(experiment_field["name"])
        elif field["name"] == "assemblies":
            for assemblies_field in field["fields"]:
                assemblies_fields.append(assemblies_field["name"])
        elif field["name"] == "analyses":
            for analysis_field in field["fields"]:
                analysis_fields.append(analysis_field["name"])

    # Collect all experiments
    raw_data = requests.get(
        f"{ENA_ROOT_URL}?accession={STUDY_ID}&result=read_run"
        f"&fields={','.join(experiment_fields)}&format=json&limit=0",
        timeout=60,
    ).json()
    # Collect all assemblies
    assemblies = requests.get(
        f"{ENA_ROOT_URL}?accession={STUDY_ID}&result=assembly"
        f"&fields={','.join(assemblies_fields)}&format=json&limit=0",
        timeout=60,
    ).json()
    # Collect all analyses
    analyses = requests.get(
        f"{ENA_ROOT_URL}?accession={STUDY_ID}&result=analysis"
        f"&fields={','.join(analysis_fields)}&format=json&limit=0",
        timeout=60,
    )

    # aggregate experiments and assemblies in the dict(key: biosample_id,
    # value: data record)
    for record_type in ["experiments", "assemblies", "analyses"]:
        aggregate_data_records(raw_data, record_type)

    # collect metadata from the BioSamples
    if PROJECT_TAG in ["ASG", "DTOL", "ERGA"]:
        first_url = (
            f"{BIOSAMPLES_ROOT_URL}?size=200&filter="
            f"attr%3Aproject%20name%3A{PROJECT_TAG}"
        )
        samples_response = requests.get(first_url, timeout=60).json()
        while "_embedded" in samples_response:
            for sample in samples_response["_embedded"]["samples"]:
                sample["project_name"] = PROJECT_TAG
                SAMPLES[sample["accession"]] = sample
            if "next" in samples_response["_links"]:
                samples_response = requests.get(
                    samples_response["_links"]["next"]["href"], timeout=60
                ).json()
            else:
                samples_response = requests.get(
                    samples_response["_links"]["last"]["href"], timeout=60
                ).json()

    # join metadata and data records
    for record_type in ["experiments", "assemblies", "analyses"]:
        join_metadata_and_data(EXPERIMENTS, PROJECT_NAME, record_type)

    # check for missing child -> parent relationship records
    additional_samples = dict()
    for sample_id, record in SAMPLES.items():
        if "sample derived from" in record["characteristics"]:
            host_sample_id = record["characteristics"]["sample derived from"][0]["text"]
            if (
                host_sample_id not in SAMPLES
                and host_sample_id not in additional_samples
            ):
                try:
                    additional_samples[host_sample_id] = requests.get(
                        f"{BIOSAMPLES_ROOT_URL}/{host_sample_id}", timeout=60
                    ).json()
                except json.decoder.JSONDecodeError:
                    print(f"json decode error for {host_sample_id}")
                    continue
        elif "sample symbiont of" in record["characteristics"]:
            host_sample_id = record["characteristics"]["sample symbiont of"][0]["text"]
            if (
                host_sample_id not in SAMPLES
                and host_sample_id not in additional_samples
            ):
                try:
                    additional_samples[host_sample_id] = requests.get(
                        f"{BIOSAMPLES_ROOT_URL}/{host_sample_id}", timeout=60
                    ).json()
                except json.decoder.JSONDecodeError:
                    print(f"json decode error for {host_sample_id}")
                    continue
    for sample_id, record in additional_samples.items():
        record["project_name"] = PROJECT_TAG
        SAMPLES[sample_id] = record

    # write results in the jsonl format
    with open(
        f"metadata_experiments_assemblies_{STUDY_ID}.jsonl", "w", encoding="utf-8"
    ) as f:
        for sample_id, record in SAMPLES.items():
            f.write(f"{json.dumps(record)}\n")


def aggregate_data_records(data_records, records_type=None):
    """
    Aggregate experiments and assemblies from the ENA into \
        python dict(key: biosample_id, value: data_record)
    :param data_records: experiments and assemblies from ENA
    :param records_type: can be of type experiment or assemblies
    """
    if records_type is None:
        raise ValueError("records_type must be specified")
    elif records_type == "experiments":
        parse_data_records(data_records, EXPERIMENTS)
    elif records_type == "assemblies":
        parse_data_records(data_records, ASSEMBLIES)
    elif records_type == "analyses":
        parse_data_records(data_records, ANALYSES)
    else:
        raise ValueError(
            "records_type must be either 'experiments' or \
                         'assemblies'"
        )


def parse_data_records(data_records, aggr_var):
    """
    Parse data records from ENA into python dict(key: biosample_id, \
        value: data_record)
    :param data_records: experiments and assemblies from ENA
    :param aggr_var: variable to aggregate data records
    """
    for record in data_records:
        if record["sample_accession"] != "":
            aggr_var[record["sample_accession"]].append(record)


def join_metadata_and_data(data_records, project_name, records_type=None):
    """
    Join records from BioSamples and ENA into python dict(key: biosample_id, \
        value: record)
    :param data_records: experiments and assemblies from ENA
    :param project_name: name of the project to import
    :param records_type: can be of type experiment or assemblies
    """
    if records_type is None:
        raise ValueError("records_type must be specified")
    if records_type not in ["experiments", "assemblies", "analyses"]:
        raise ValueError(
            "records_type must be either 'experiments' or \
                         'assemblies'"
        )
    else:
        for sample_id, data in data_records.items():
            if sample_id not in SAMPLES:
                try:
                    response = requests.get(
                        f"{BIOSAMPLES_ROOT_URL}/{sample_id}", timeout=60
                    ).json()
                    if response["status"] == 403:
                        continue
                    SAMPLES[sample_id] = response
                    SAMPLES[sample_id]["project_name"] = project_name
                    SAMPLES[sample_id][records_type] = data
                except requests.exceptions.JSONDecodeError:
                    continue
            else:
                SAMPLES[sample_id].setdefault(records_type, [])
                SAMPLES[sample_id][records_type].extend(data)
                SAMPLES[sample_id]["project_name"] = project_name


if __name__ == "__main__":
    main()
