import requests
import json

from collections import defaultdict
from samples_schema import metadata_schema

DToL_STUDY_ID = "PRJEB40665"
DToL_EXPERIMENTS = defaultdict(list)
DToL_ASSEMBLIES = defaultdict(list)
DToL_SAMPLES = dict()


def main():
    """
    Collect DToL metadata from BioSamples, experiments and assemblies from the ENA
    :return:
    """
    # collect required experiments and assemblies fields
    experiment_fields = list()
    assemblies_fields = list()
    for field in metadata_schema["fields"]:
        if field["name"] == "experiments":
            for experiment_field in field["fields"]:
                experiment_fields.append(experiment_field["name"])
        elif field["name"] == "assemblies":
            for assemblies_field in field["fields"]:
                assemblies_fields.append(assemblies_field["name"])

    # Collect all experiments
    raw_data = requests.get(
        f"https://www.ebi.ac.uk/ena/portal/api/filereport?accession={DToL_STUDY_ID}&result=read_run"
        f"&fields={','.join(experiment_fields)}&format=json&limit=0").json()
    # Collect all assemblies
    assemblies = requests.get(
        f"https://www.ebi.ac.uk/ena/portal/api/filereport?accession={DToL_STUDY_ID}&result=assembly"
        f"&fields={','.join(assemblies_fields)}&format=json&limit=0").json()

    # aggregate experiments and assemblies in the dict(key: biosample_id, value: data record)
    aggregate_data_records(raw_data, "experiments")
    aggregate_data_records(assemblies, "assemblies")

    # collect metadata from the BioSamples
    # TODO: consider using AsyncIO to improve the speed
    first_url = "https://www.ebi.ac.uk/biosamples/samples?size=200&filter=attr%3Aproject%20name%3ADTOL"
    samples_response = requests.get(first_url).json()
    while '_embedded' in samples_response:
        for sample in samples_response['_embedded']['samples']:
            DToL_SAMPLES[sample['accession']] = sample
        if 'next' in samples_response['_links']:
            samples_response = requests.get(samples_response['_links']['next']['href']).json()
        else:
            samples_response = requests.get(samples_response['_links']['last']['href']).json()

    # join metadata and data records
    join_metadata_and_data(DToL_EXPERIMENTS, "experiments")
    join_metadata_and_data(DToL_ASSEMBLIES, "assemblies")

    # check for missing child -> parent relationship records
    additional_samples = dict()
    for sample_id, record in DToL_SAMPLES.items():
        if 'sample derived from' in record['characteristics']:
            host_sample_id = record['characteristics']['sample derived from'][0]['text']
            if host_sample_id not in DToL_SAMPLES and host_sample_id not in additional_samples:
                additional_samples[host_sample_id] = requests.get(
                    f"https://www.ebi.ac.uk/biosamples/samples/{host_sample_id}").json()
    for sample_id, record in additional_samples.items():
        DToL_SAMPLES[sample_id] = record

    # write results in the jsonl format
    with open("dtol_data.jsonl", "w") as f:
        for sample_id, record in DToL_SAMPLES.items():
            f.write(f"{json.dumps(record)}\n")


def aggregate_data_records(data_records, records_type=None):
    """
    Aggregate experiments and assemblies from the ENA into python dict(key: biosample_id, value: data_record)
    :param data_records: experiments and assemblies from ENA
    :param records_type: can be of type experiment or assemblies
    """
    if records_type is None:
        raise ValueError("records_type must be specified")
    elif records_type == 'experiments':
        parse_data_records(data_records, DToL_EXPERIMENTS)
    elif records_type == 'assemblies':
        parse_data_records(data_records, DToL_ASSEMBLIES)
    else:
        raise ValueError("records_type must be either 'experiments' or 'assemblies'")


def parse_data_records(data_records, aggr_var):
    """
    Parse data records from ENA into python dict(key: biosample_id, value: data_record)
    :param data_records: experiments and assemblies from ENA
    :param aggr_var: variable to aggregate data records
    """
    for record in data_records:
        aggr_var[record["sample_accession"]].append(record)


def join_metadata_and_data(data_records, records_type=None):
    """
    Join records from BioSamples and ENA into python dict(key: biosample_id, value: record)
    :param data_records: experiments and assemblies from ENA
    :param records_type: can be of type experiment or assemblies
    """
    if records_type is None:
        raise ValueError("records_type must be specified")
    elif records_type not in ["experiments", "assemblies"]:
        raise ValueError("records_type must be either 'experiments' or 'assemblies'")
    else:
        for sample_id, data in data_records.items():
            if sample_id not in DToL_SAMPLES:
                DToL_SAMPLES[sample_id] = requests.get(f"https://www.ebi.ac.uk/biosamples/samples/{sample_id}").json()
                DToL_SAMPLES[sample_id][records_type] = data
            else:
                DToL_SAMPLES[sample_id][records_type] = data


if __name__ == "__main__":
    main()
