# IGTI Bootcamp - Cloud Data Engineer - Challenge 1

<p align="center">
    <a href="#description">Description</a> &bull;
    <a href="#stack">Stack</a> &bull;
    <a href="#diagram">Diagram</a> &bull;
</p>

## Description
Create a process to start the infrastructure, ingest, read, transform (to Parquet file), processing and manipulate files on DataLake.
The resources were created and destroyed by Terraform pipeline.

- Data Lake on GCP Cloud Storage.
- Job Spark (PySpark) on GCP Cloud DataProc.
- GCP BigQuery can be used to get insights querying the Data Lake (Parquet Files).

## Stack
- Python
- PySpark
- Terraform
- Google Cloud Platform (Cloud Storage, Cloud Dataproc)

## Diagram
![diagram](https://github.com/mralmeidars/igti-cloud-data-engineer-1-gcp/blob/master/docs/Infrastructure_Diagram.png)
