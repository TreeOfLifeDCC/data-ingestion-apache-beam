# Biodiversity Data Ingestion pipeline using Apache Beam

to run the pipeline in GCP:
```shell
export PROJECT_ID=$(gcloud config get-value project)

python3 pipeline.py --project=${PROJECT_ID} --region=europe-west2 
--stagingLocation=gs://$PROJECT_ID/staging/ --tempLocation=gs://$PROJECT_ID/temp/ 
--runner=DataflowRunner --disk_size_gb=30 --machine_type=e2-medium
```