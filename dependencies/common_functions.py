import requests
import gzip
import apache_beam as beam
from urllib.request import urlretrieve
from collections import defaultdict
from lxml import etree

PHYLOGENETIC_RANKS = ('kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species')


def parse_annotations(sample):
    sample_to_return = dict()
    sample_to_return["record_type"] = sample["record_type"]
    sample_to_return["accession"] = sample["accession"]
    info = sample["info"].split(";")
    for item in info:
        if item:
            try:
                k, v = item.split()
                if k in ["gene_id", "gene_version", "gene_source", "gene_biotype", "transcript_id",
                         "transcript_version", "transcript_source", "transcript_biotype", "tag", "exon_number",
                         "exon_id", "exon_version", "protein_id", "protein_version", "gene_name"]:
                    sample_to_return[k] = v.replace('"', '')
            except ValueError:
                pass
    return sample_to_return


def classify_samples(sample):
    error_sample = dict()
    error_sample['biosample_id'] = sample["accession"]
    try:
        checklist = sample["characteristics"]["ENA-CHECKLIST"][0]["text"]
        if checklist in SPECIMENS_SYMBIONTS_CHECKLISTS:
            if ("symbiont" in sample["characteristics"]
                    and sample["characteristics"]["symbiont"][0]["text"] == "Y"):
                return beam.pvalue.TaggedOutput("Symbionts", sample)
            else:
                return sample
        elif checklist in METAGENOMES_CHECKLISTS:
            return beam.pvalue.TaggedOutput("Metagenomes", sample)
        else:
            error_sample['checklist'] = checklist
            return beam.pvalue.TaggedOutput("Errors", error_sample)
    except KeyError:
        error_sample['checklist'] = "Wasn't found"
        return beam.pvalue.TaggedOutput("Errors", error_sample)


def check_fieldxistence(record):
    values = list()
    units = list()
    ontology_terms = list()
    for element in record:
        values.append(element["text"])
        try:
            units.append(element["unit"])
        except KeyError:
            pass
        try:
            ontology_terms.append(element["ontologyTerms"][0])
        except KeyError:
            pass
    return ", ".join(values), ", ".join(units), ", ".join(ontology_terms)


def process_records_for_dwh(sample, sample_type):
    error_sample = dict()
    error_sample['biosample_id'] = sample["accession"]

    dwh_record = dict()
    dwh_record['accession'] = sample['accession']

    try:
        organism_name, organism_ontology, _ = check_field_existence(sample['characteristics']['organism'])
        dwh_record['organism'] = {'text': organism_name, 'ontologyTerm': organism_ontology}
    except KeyError:
        dwh_record['organism'] = {'text': None, 'ontologyTerm': None}

    if dwh_record['organism']['text']:
        dwh_record['commonName'] = get_common_name(dwh_record['organism']['text'])
    else:
        dwh_record['commonName'] = None

    try:
        sex_name, _, _ = check_field_existence(sample['characteristics']['sex'])
        dwh_record['sex'] = sex_name
    except KeyError:
        dwh_record['sex'] = None

    try:
        organism_part_name, _, _ = check_field_existence(sample['characteristics']['organism part'])
        dwh_record['organismPart'] = organism_part_name
    except KeyError:
        dwh_record['organismPart'] = None

    try:
        tolid, _, _ = check_field_existence(sample['characteristics']['tolid'])
        dwh_record['tolid'] = tolid
    except KeyError:
        dwh_record['tolid'] = None

    try:
        lat, _, _ = check_field_existence(sample['characteristics']['geographic location (latitude)'])
        dwh_record['lat'] = lat
    except KeyError:
        dwh_record['lat'] = None

    try:
        lon, _, _ = check_field_existence(sample['characteristics']['geographic location (longitude)'])
        dwh_record['lon'] = lon
    except KeyError:
        dwh_record['lon'] = None

    try:
        locality, _, _ = check_field_existence(sample['characteristics']['geographic location (region and locality)'])
        dwh_record['locality'] = locality
    except KeyError:
        dwh_record['locality'] = None

    if "experiments" in sample and len(sample["experiments"]) > 0:
        dwh_record["trackingSystem"] = "Raw Data - Submitted"
    elif "assemblies" in sample and len(sample["assemblies"]) > 0:
        dwh_record["trackingSystem"] = "Assemblies - Submitted"
    else:
        dwh_record["trackingSystem"] = "Submitted to BioSamples"

    try:
        dwh_record["experiments"] = sample["experiments"]
    except KeyError:
        dwh_record["experiments"] = list()
    try:
        dwh_record["assemblies"] = sample["assemblies"]
    except KeyError:
        dwh_record["assemblies"] = list()

    if sample_type == 'specimens':
        return sample['taxId'], dwh_record
    elif sample_type == "symbionts":
        try:
            host_biosample_id = sample['characteristics']['sample symbiont of'][0]['text']
        except KeyError:
            error_sample['error_message'] = "missing 'sample symbiont of' field for symbiont sample"
            return beam.pvalue.TaggedOutput("Errors", error_sample)
        host_sample = requests.get(f"https://www.ebi.ac.uk/biosamples/samples/{host_biosample_id}.json").json()
        return host_sample['taxId'], dwh_record
    else:
        try:
            host_biosample_id = sample['characteristics']['sample derived from'][0]['text']
        except KeyError:
            error_sample['error_message'] = "missing 'sample derived from' field for metagenome sample"
            return beam.pvalue.TaggedOutput("Errors", error_sample)
        host_sample = requests.get(f"https://www.ebi.ac.uk/biosamples/samples/{host_biosample_id}.json").json()
        while host_sample["characteristics"]["ENA-CHECKLIST"][0]["text"] != "ERC000053":
            try:
                host_biosample_id = host_sample['characteristics']['sample derived from'][0]['text']
            except KeyError:
                error_sample['error_message'] = "missing 'sample derived from' field for metagenome sample"
                return beam.pvalue.TaggedOutput("Errors", error_sample)
            host_sample = requests.get(f"https://www.ebi.ac.uk/biosamples/samples/{host_biosample_id}.json").json()
        return host_sample['taxId'], dwh_record


def process_specimens_for_data_portal(sample):
    data_portal_record = dict()
    data_portal_record["biosample_id"] = sample['accession']
    if "experiments" in sample and len(sample["experiments"]) > 0:
        data_portal_record["tracking_status"] = "Raw Data - Submitted"
    elif "assemblies" in sample and len(sample["assemblies"]) > 0:
        data_portal_record["tracking_status"] = "Assemblies - Submitted"
    else:
        data_portal_record["tracking_status"] = "Submitted to BioSamples"
    data_portal_record["characteristics"] = list()
    for record_name, record in sample['characteristics'].items():
        values, units, ontology_terms = check_field_existence(record)
        data_portal_record["characteristics"].append({
            "field_name": record_name,
            "field_value": values,
            "unit": units,
            "ontology_term": ontology_terms
        })
    if "relationships" in sample and len(sample["relationships"]) > 0:
        data_portal_record["relationships"] = sample["relationships"]
    return data_portal_record


def get_reads(sample):
    if 'accession' in sample:
        sample_id = sample['accession']
        tax_id = sample['taxId']
    else:
        sample_id = sample[1]['biosample_id']
        tax_id = sample[0]
    experiments_data = requests.get(f'https://www.ebi.ac.uk/ena/portal/'
                                    f'api/filereport?result=read_run'
                                    f'&accession={sample_id}'
                                    f'&offset=0&limit=1000&format=json'
                                    f'&fields=study_accession,'
                                    f'secondary_study_accession,'
                                    f'sample_accession,'
                                    f'secondary_sample_accession,'
                                    f'experiment_accession,run_accession,'
                                    f'submission_accession,tax_id,'
                                    f'scientific_name,instrument_platform,'
                                    f'instrument_model,library_name,'
                                    f'nominal_length,library_layout,'
                                    f'library_strategy,library_source,'
                                    f'library_selection,read_count,'
                                    f'base_count,center_name,first_public,'
                                    f'last_updated,experiment_title,'
                                    f'study_title,study_alias,'
                                    f'experiment_alias,run_alias,'
                                    f'fastq_bytes,fastq_md5,fastq_ftp,'
                                    f'fastq_aspera,fastq_galaxy,'
                                    f'submitted_bytes,submitted_md5,'
                                    f'submitted_ftp,submitted_aspera,'
                                    f'submitted_galaxy,submitted_format,'
                                    f'sra_bytes,sra_md5,sra_ftp,sra_aspera,'
                                    f'sra_galaxy,'
                                    f'sample_alias,broker_name,'
                                    f'sample_title,nominal_sdev,'
                                    f'first_created')
    if experiments_data.status_code != 200:
        return (tax_id, list())
    else:
        experiments = experiments_data.json()
        for experiment in experiments:
            response = requests.get(f"https://www.ebi.ac.uk/ena/browser/api/xml/{experiment['experiment_accession']}")
            root = etree.fromstring(response.content)
            try:
                experiment['library_construction_protocol'] = root.find('EXPERIMENT').find('DESIGN').find(
                    'LIBRARY_DESCRIPTOR').find('LIBRARY_CONSTRUCTION_PROTOCOL').text
            except AttributeError:
                experiment['library_construction_protocol'] = 'Not specified'

        return (tax_id, experiments)


def merge_data_records(sample):
    data = list()
    for record in sample[1]:
        data.extend(record)
    return (sample[0], data)


def get_common_name(latin_name):
    common_name_response = requests.get(
        f"https://www.ebi.ac.uk/ena/taxonomy/rest/scientific-name/{latin_name}")
    if common_name_response.content.decode('utf-8') == "No results.":
        return 'Not specified'
    common_name_response = common_name_response.json()
    if len(common_name_response) != 0 and 'commonName' in common_name_response[0]:
        return common_name_response[0]['commonName']
    else:
        return 'Not specified'


def final_formatting(element):
    phylogenetic_ranks = ('kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species', 'cohort', 'forma',
                          'infraclass', 'infraorder', 'parvorder', 'section', 'series', 'species_group',
                          'species_subgroup', 'subclass', 'subcohort', 'subfamily', 'subgenus', 'subkingdom',
                          'suborder', 'subphylum', 'subsection', 'subspecies', 'subtribe', 'superclass', 'superfamily',
                          'superkingdom', 'superorder', 'superphylum', 'tribe', 'varietas')
    sample = dict()
    # host metadata
    sample['tax_id'] = element[0]
    sample['organism'] = element[1]['specimens'][0]['organism']['text']
    sample['commonName'] = element[1]['specimens'][0]['commonName']
    sample['currentStatus'] = 'Submitted to BioSamples'

    sample['experiment'], sample['assemblies'] = parse_data_records(element[1]['specimens'])
    sample['records'] = element[1]['specimens']

    sample['taxonomies'] = dict()
    # adding phylogenetic information
    response = requests.get(f"https://www.ebi.ac.uk/ena/browser/api/xml/{sample['tax_id']}")
    root = etree.fromstring(response.content)
    for rank in phylogenetic_ranks:
        sample['taxonomies'][rank] = {
            "scientificName": 'Other',
            "commonName": 'Other',
            "tax_id": None
        }

    try:
        for taxon in root.find('taxon').find('lineage').findall('taxon'):
            rank = taxon.get('rank')
            if rank in phylogenetic_ranks:
                scientific_name = taxon.get('scientificName')
                common_name = taxon.get('commonName')
                tax_id = taxon.get('taxId')
                sample['taxonomies'][rank]['scientificName'] = scientific_name if scientific_name else 'Other'
                sample['taxonomies'][rank]['commonName'] = common_name if common_name else 'Other'
                sample['taxonomies'][rank]['tax_id'] = tax_id if tax_id else None
    except AttributeError:
        error_sample = {
            "tax_id": sample['tax_id'],
            "scientific_name": sample['organism'],
            "common_name": sample['commonName']}
        return beam.pvalue.TaggedOutput('error', error_sample)

    # update phylogenetic tree names
    # sample['phylogenetic_tree_scientific_names'] = list()
    # sample['phylogenetic_tree_common_names'] = list()
    # for rank in phylogenetic_ranks:
    #     sample['phylogenetic_tree_scientific_names'].append(sample['phylogenetic_tree'][rank]['scientific_name'])
    #     sample['phylogenetic_tree_common_names'].append(sample['phylogenetic_tree'][rank]['common_name'])

    # symbionts and metagenomes raw data
    sample['symbionts_experiment'], sample['symbionts_assemblies'] = parse_data_records(element[1]['symbionts'])
    sample['symbionts_records'] = element[1]['symbionts']
    sample['metagenomes_experiment'], sample['metagenomes_assemblies'] = parse_data_records(element[1]['metagenomes'])
    sample['metagenomes_records'] = element[1]['metagenomes']
    if len(sample['symbionts_records']) > 0:
        sample['symbionts_biosamples_status'] = 'Submitted to BioSamples'
    if len(sample['symbionts_assemblies']) > 0:
        sample['symbionts_assemblies_status'] = 'Assemblies Submitted'


    # host data status
    sample['currentStatus'] = 'Raw Data - Submitted' if len(sample['experiment']) != 0 else sample['currentStatus']
    sample['currentStatus'] = 'Assemblies - Submitted' if len(sample['assemblies']) != 0 else sample['currentStatus']

    data_portal_response = requests.get(
        f"https://portal.darwintreeoflife.org/api/root_organisms/root?id={sample['organism']}").json()
    if 'annotation' in data_portal_response and len(data_portal_response['annotation']) > 0:
        sample['currentStatus'] = 'Annotation Complete'
        sample['annotation'] = data_portal_response['annotation']

    sample['biosamples'] = 'Done'
    sample['annotation_status'] = 'Waiting'
    if sample['currentStatus'] == 'Annotation Complete':
        sample['annotation_complete'] = 'Done'
    else:
        sample['annotation_complete'] = 'Waiting'
    if len(sample['assemblies']) > 0:
        sample['assemblies_status'] = 'Done'
    else:
        sample['assemblies_status'] = 'Waiting'
    if len(sample['experiment']) > 0:
        sample['mapped_reads'] = 'Done'
        sample['raw_data'] = 'Done'
    else:
        sample['mapped_reads'] = 'Waiting'
        sample['raw_data'] = 'Waiting'
    sample['trackingSystem'] = [
        {'name': 'biosamples', 'status': sample['biosamples'], 'rank': 1},
        {'name': 'mapped_reads', 'status': sample['mapped_reads'], 'rank': 2},
        {'name': 'assemblies', 'status': sample['assemblies_status'], 'rank': 3},
        {'name': 'raw_data', 'status': sample['raw_data'], 'rank': 4},
        {'name': 'annotation', 'status': sample['annotation_status'], 'rank': 5},
        {'name': 'annotation_complete', 'status': sample['annotation_complete'], 'rank': 6}]

    sample['tolid'] = set()
    # TODO: collect geo coordinates
    sample['orgGeoList'] = list()
    sample['specGeoList'] = list()
    for specimen in element[1]['specimens']:
        sample['tolid'].add(specimen['tolid'])
        tmp = dict()
        tmp['organism'] = specimen['organism']['text']
        tmp['accession'] = specimen['accession']
        tmp['commonName'] = specimen['commonName']
        tmp['sex'] = specimen['sex']
        tmp['organismPart'] = specimen['organismPart']
        tmp['lat'] = specimen['lat']
        tmp['lng'] = specimen['lon']
        tmp['locality'] = specimen['locality']
        sample['orgGeoList'].append(tmp)
    sample['tolid'] = list(sample['tolid'])

    sample['genome_notes'] = list()
    if 'genome_notes' in data_portal_response and len(data_portal_response['genome_notes']) > 0:
        sample['genome_notes'] = data_portal_response['genome_notes']

    sample['goat_info'] = dict()
    if 'goat_info' in data_portal_response:
        sample['goat_info'] = data_portal_response['goat_info']

    # TODO: collect other common names
    sample['commonNameSource'] = 'NCBI_taxon'
    if 'commonName' in data_portal_response:
        sample['commonName'] = data_portal_response['commonName']
    if 'commonNameSource' in data_portal_response:
        sample['commonNameSource'] = data_portal_response['commonNameSource']

    sample['show_tolqc'] = False
    if 'show_tolqc' in data_portal_response:
        sample['show_tolqc'] = data_portal_response['show_tolqc']

    sample['nbnatlas'] = None
    if 'nbnatlas' in data_portal_response:
        sample['nbnatlas'] = data_portal_response['nbnatlas']

    return beam.pvalue.TaggedOutput('normal', sample)


def parse_data_records(records):
    experiments = list()
    assemblies = list()
    for record in records:
        if 'experiments' in record:
            experiments.extend(record['experiments'])
            del record['experiments']
        if 'assemblies' in record:
            assemblies.extend(record['assemblies'])
            del record['assemblies']
    return experiments, assemblies
