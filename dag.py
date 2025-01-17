import pendulum
import json

from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath

from dependencies.biodiversity_projects import (
    gbdp_projects,
    erga_projects,
    dtol_projects,
    asg_projects,
)


@task
def get_metadata(study_id: str, project_name: str, bucket_name: str, **kwargs) -> None:
    import collect_metadata_experiments_assemblies

    if "ERGA" in project_name:
        project_tag = "ERGA"
    else:
        project_tag = project_name
    metadata = collect_metadata_experiments_assemblies.main(
        study_id, project_tag, project_name
    )

    base = ObjectStoragePath(f"gs://google_cloud_default@{bucket_name}")
    base.mkdir(exist_ok=True)
    path = base / f"{study_id}.jsonl"
    with path.open("w") as file:
        for sample_id, record in metadata.items():
            file.write(f"{json.dumps(record)}\n")


@task
def start_apache_beam_gbdp(**kwargs) -> None:
    print("DONE")


@task
def start_apache_beam_erga(**kwargs) -> None:
    print("DONE")


@task
def start_apache_beam_dtol(**kwargs) -> None:
    print("DONE")


@task
def start_apache_beam_asg(**kwargs) -> None:
    print("DONE")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["biodiversity_metadata_ingestion"],
)
def biodiversity_metadata_ingestion():
    """
    This DAG builds BigQuery tables and ElasticSearch indexes for all
    biodiversity projects
    """
    gbdp_metadata_import_tasks = []
    for study_id, item in gbdp_projects.items():
        project_name, bucket_name = item["project_name"], item["bucket_name"]
        gbdp_metadata_import_tasks.append(
            get_metadata.override(task_id=f"gbdp_{study_id}_get_metadata")(
                study_id, project_name, bucket_name
            )
        )
    gbdp_metadata_import_tasks >> start_apache_beam_gbdp()

    erga_metadata_import_tasks = []
    for study_id, item in erga_projects.items():
        project_name, bucket_name = item["project_name"], item["bucket_name"]
        erga_metadata_import_tasks.append(
            get_metadata.override(task_id=f"erga_{study_id}_get_metadata")(
                study_id, project_name, bucket_name
            )
        )
    erga_metadata_import_tasks >> start_apache_beam_erga()

    dtol_metadata_import_tasks = []
    for study_id, item in dtol_projects.items():
        project_name, bucket_name = item["project_name"], item["bucket_name"]
        dtol_metadata_import_tasks.append(
            get_metadata.override(task_id=f"dtol_{study_id}_get_metadata")(
                study_id, project_name, bucket_name
            )
        )
    dtol_metadata_import_tasks >> start_apache_beam_dtol()

    asg_metadata_import_tasks = []
    for study_id, item in asg_projects.items():
        project_name, bucket_name = item["project_name"], item["bucket_name"]
        asg_metadata_import_tasks.append(
            get_metadata.override(task_id=f"asg_{study_id}_get_metadata")(
                study_id, project_name, bucket_name
            )
        )
    asg_metadata_import_tasks >> start_apache_beam_asg()


biodiversity_metadata_ingestion()
