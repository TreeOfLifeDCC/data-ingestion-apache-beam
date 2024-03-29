import requests
import apache_beam as beam
from lxml import etree

# ParDo class for samples classification
class ClassifySamples(beam.DoFn):
    def process(self, element, *args, **kwargs):
        if 'ENA-CHECKLIST' not in element["characteristics"] or element["characteristics"]["ENA-CHECKLIST"][0]["text"] in ["ERC000053", "ERC000011"]:
            # TODO: check for 'sample symbiont of'
            if "symbiont" in element["characteristics"] and element["characteristics"]["symbiont"][0]["text"] == "Y":
                return [beam.pvalue.TaggedOutput("Symbionts", element)]
            else:
                if "sample derived from" in element["characteristics"]:
                    return [beam.pvalue.TaggedOutput("Specimens", element)]
                else:
                    return [element]
        else:
            # TODO: check for 'sample derived from'
            return [beam.pvalue.TaggedOutput("Metagenomes", element)]


# Helper functions
def check_field_existence(sample, field_name, units=False, ontology=False):
    if units:
        if field_name in sample:
            return {
                'text': sample[field_name][0]['text'],
                'unit': sample[field_name][0]['unit']
            }
        else:
            return {
                'text': None,
                'unit': None
            }
    elif ontology:
        if field_name in sample:
            return {
                'text': sample[field_name][0]['text'],
                'ontologyTerm': sample[field_name][0]['ontologyTerms'][0]
            }
        else:
            return {
                'text': None,
                'unit': None
            }
    else:
        if field_name in sample:
            return sample[field_name][0]['text']
        else:
            return None


# Processing for specimens and organisms
def process_organisms_specimens(sample):
    record = dict()
    record['biosample_id'] = sample['accession']
    record['organism'] = check_field_existence(sample['characteristics'], 'organism')
    # TODO: check if this name is not None
    record['common_name'] = get_common_name(record['organism'])
    record['sex'] = check_field_existence(sample['characteristics'], 'sex')
    record['organism_part'] = check_field_existence(sample['characteristics'], 'organism part')
    record['latitude'] = check_field_existence(sample['characteristics'], 'geographic location (latitude)')
    record['longitude'] = check_field_existence(sample['characteristics'], 'geographic location (longitude)')
    return (sample['taxId'], record)


# Processing for symbionts
def process_symbiont(sample):
    host_biosample_id = sample['characteristics']['sample symbiont of'][0]['text']
    host_sample = requests.get(f"https://www.ebi.ac.uk/biosamples/samples/{host_biosample_id}.json").json()
    record = dict()
    record['biosample_id'] = sample['accession']
    record['organism'] = check_field_existence(sample['characteristics'], 'organism')
    record['common_name'] = get_common_name(record['organism'])
    record['sex'] = check_field_existence(sample['characteristics'], 'sex')
    record['organism_part'] = check_field_existence(sample['characteristics'], 'organism part')
    return (host_sample['taxId'], record)


# Processing for metagenomes
def process_metagenomes(sample):
    host_biosample_id = sample['characteristics']['sample derived from'][0]['text']
    host_sample = requests.get(f"https://www.ebi.ac.uk/biosamples/samples/{host_biosample_id}.json").json()
    while host_sample["characteristics"]["ENA-CHECKLIST"][0]["text"] != "ERC000053":
        host_biosample_id = host_sample['characteristics']['sample derived from'][0]['text']
        host_sample = requests.get(f"https://www.ebi.ac.uk/biosamples/samples/{host_biosample_id}.json").json()
    record = dict()
    record['biosample_id'] = sample['accession']
    record['organism'] = check_field_existence(sample['characteristics'], 'organism')
    record['common_name'] = get_common_name(record['organism'])
    record['sex'] = check_field_existence(sample['characteristics'], 'sex')
    record['organism_part'] = check_field_existence(sample['characteristics'], 'organism part')
    return (host_sample['taxId'], record)


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


def parse_assemblies(sample):
    if 'accession' in sample:
        sample_id = sample['accession']
        tax_id = sample['taxId']
    else:
        sample_id = sample[1]['biosample_id']
        tax_id = sample[0]
    assemblies_data = requests.get(f"https://www.ebi.ac.uk/ena/portal/api/"
                                   f"links/sample?format=json"
                                   f"&accession={sample_id}&result=assembly"
                                   f"&offset=0&limit=1000")
    if assemblies_data.status_code != 200:
        return (tax_id, list())
    else:
        return (tax_id, assemblies_data.json())


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
    phylogenetic_ranks = ('kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species')
    sample = dict()
    # host metadata
    sample['tax_id'] = element[0]
    try:
        sample['scientific_name'] = element[1]['organisms'][0]['organism']
    except IndexError:
        sample['organisms'] = element[1]['organisms']
        sample['specimens'] = element[1]['specimens']
        sample['symbionts'] = element[1]['symbionts']
        sample['metagenomes'] = element[1]['metagenomes']
        sample['error_message'] = "Species without organisms"
        return beam.pvalue.TaggedOutput('error', sample)
    sample['common_name'] = element[1]['organisms'][0]['common_name']
    sample['current_status'] = 'Submitted to BioSamples'
    sample['organisms'] = element[1]['organisms']
    sample['specimens'] = element[1]['specimens']
    sample['phylogenetic_tree'] = dict()

    # adding phylogenetic information
    response = requests.get(f"https://www.ebi.ac.uk/ena/browser/api/xml/{sample['tax_id']}")
    root = etree.fromstring(response.content)
    for rank in phylogenetic_ranks:
        sample['phylogenetic_tree'][rank] = {
            "scientific_name": 'Not specified',
            "common_name": 'Not specified'
        }

    try:
        for taxon in root.find('taxon').find('lineage').findall('taxon'):
            rank = taxon.get('rank')
            if rank in phylogenetic_ranks:
                scientific_name = taxon.get('scientificName')
                common_name = taxon.get('commonName')
                sample['phylogenetic_tree'][rank][
                    'scientific_name'] = scientific_name if scientific_name else 'Not specified'
                sample['phylogenetic_tree'][rank]['common_name'] = common_name if common_name else 'Not specified'
    except AttributeError:
        sample['error_message'] = "Species without phylogeny"
        return beam.pvalue.TaggedOutput('error', sample)

    # update phylogenetic tree names
    sample['phylogenetic_tree_scientific_names'] = list()
    sample['phylogenetic_tree_common_names'] = list()
    for rank in phylogenetic_ranks:
        sample['phylogenetic_tree_scientific_names'].append(sample['phylogenetic_tree'][rank]['scientific_name'])
        sample['phylogenetic_tree_common_names'].append(sample['phylogenetic_tree'][rank]['common_name'])

    # symbionts and metagenomes raw data
    sample['symbionts'] = element[1]['symbionts']
    sample['metagenomes'] = element[1]['metagenomes']

    # host data
    sample['raw_data'] = [experiment for item in element[1]['raw_data'] for experiment in item]
    sample['assemblies'] = [assembly for item in element[1]['assemblies'] for assembly in item]

    # symbionts and metagenomes data
    sample['symbionts_raw_data'] = [experiment for item in element[1]['symbionts_raw_data'] for experiment in item]
    sample['symbionts_assemblies'] = [assembly for item in element[1]['symbionts_assemblies'] for assembly in item]
    sample['metagenomes_raw_data'] = [experiment for item in element[1]['metagenomes_raw_data'] for experiment in item]
    sample['metagenomes_assemblies'] = [assembly for item in element[1]['metagenomes_assemblies'] for assembly in item]

    # symbionts and metagenomes metadata status
    sample['symbionts_status'] = 'Symbionts Submitted to BioSamples' if len(
        sample['symbionts']) != 0 else 'Not available'
    sample['metagenomes_status'] = 'Metagenomes Submitted to Biosamples' if len(
        sample['metagenomes']) != 0 else 'Not available'

    # host data status
    sample['current_status'] = 'Raw Data - Submitted' if len(sample['raw_data']) != 0 else sample['current_status']
    sample['current_status'] = 'Assemblies - Submitted' if len(sample['assemblies']) != 0 else sample['current_status']

    data_portal_response = requests.get(
        f"https://portal.darwintreeoflife.org/api/root_organisms/root?id={sample['scientific_name']}").json()
    if 'annotation' in data_portal_response and len(data_portal_response['annotation']) > 0:
        sample['current_status'] = 'Annotation Complete'

    # symbionts and metagenomes data status
    sample['symbionts_status'] = 'Symbionts Raw Data - Submitted' if len(sample['symbionts_raw_data']) != 0 else sample[
        'symbionts_status']
    sample['symbionts_status'] = 'Symbionts Assemblies - Submitted' if len(sample['symbionts_assemblies']) != 0 else \
    sample['symbionts_status']
    sample['metagenomes_status'] = 'Metagenomes Raw Data - Submitted' if len(sample['metagenomes_raw_data']) != 0 else \
    sample['metagenomes_status']
    sample['metagenomes_status'] = 'Metagenomes Assemblies - Submitted' if len(
        sample['metagenomes_assemblies']) != 0 else sample['metagenomes_status']

    return beam.pvalue.TaggedOutput('normal', sample)