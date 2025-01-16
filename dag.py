import pendulum
import json

from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath

from dependencies.biodiversity_projects import biodiversity_projects


@task
def get_metadata(
        study_id: str, project_name: str, buckets_names: list, **kwargs) -> None:
    import collect_metadata_experiments_assemblies

    project_tag = project_name
    metadata = collect_metadata_experiments_assemblies.main(
        study_id, project_tag, project_name)

    for bucket_name in buckets_names:
        base = ObjectStoragePath(f"gs://google_cloud_default@{bucket_name}")
        base.mkdir(exist_ok=True)
        path = base / f"{study_id}.jsonl"
        with path.open("w") as file:
            for sample_id, record in metadata.items():
                file.write(f"{json.dumps(record)}\n")

@task
def end(**kwargs) -> None:
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
    metadata_import_tasks = []
    for study_id, item in biodiversity_projects.items():
        project_name = item[0]
        buckets_names = item[1]
        metadata_import_tasks.append(get_metadata.override(
            task_id=f"{study_id}_get_metadata")(
            study_id, project_name, buckets_names))
    metadata_import_tasks >> end()
biodiversity_metadata_ingestion()