import requests
import json
from lxml import etree

ASG_URL = "https://portal.aquaticsymbiosisgenomics.org/api/data_portal_test"
ENA_URL = "https://www.ebi.ac.uk/ena"
RESULTS = []


def main():
    offset = 0
    data = list()

    # First request
    response = requests.get(f"{ASG_URL}?offset={offset}&limit=10").json()
    while offset + 10 <= response["count"]:
        data.extend(response["results"])
        offset += 10
        response = requests.get(f"{ASG_URL}?offset={offset}&limit=10").json()
    # Last request to collect remaining records
    response = requests.get(f"{ASG_URL}?offset={offset}&limit=10").json()
    data.extend(response["results"])

    for i, record in enumerate(data):
        print(
            f"Processing data: {round(i / len(data) * 100, 2)}%\r", end="", flush=True
        )
        RESULTS.append(collect_analyses_ids(record["_source"]))


def collect_analyses_ids(record):
    """
    Collect all analyses IDs (ERZs) from ES record.
    record: ES entry
    """
    tmp = dict()
    tmp["organism_name"] = record["organism"]
    for assembly_type in [
        "assemblies",
        "metagenomes_assemblies",
        "symbionts_assemblies",
    ]:
        tmp[assembly_type] = []
        if assembly_type in record and len(record[assembly_type]) > 0:
            for assmbl in record[assembly_type]:
                tmp2 = {assmbl["accession"]: []}
                response = requests.get(
                    f"{ENA_URL}/browser/api/xml/{assmbl['accession']}"
                )
                root = etree.fromstring(response.content)
                project_id = (
                    root.find("ASSEMBLY")
                    .find("STUDY_REF")
                    .find("IDENTIFIERS")
                    .find("PRIMARY_ID")
                    .text
                )
                response = requests.get(
                    f"{ENA_URL}/portal/api/filereport?accession="
                    f"{project_id}&result=analysis&fields=analysis_accession&"
                    f"format=json&download=true&limit=0"
                ).json()
                for erz in response:
                    tmp2[assmbl["accession"]].append(erz["analysis_accession"])
            tmp[assembly_type].append(tmp2)
    return tmp


if __name__ == "__main__":
    main()
    with open("asg_mgnify_assemblies.json", "w") as outfile:
        json.dump(RESULTS, outfile)
