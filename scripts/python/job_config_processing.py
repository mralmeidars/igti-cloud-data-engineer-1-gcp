import os
import re
#from google.auth import credentials
#from google.auth.credentials import Credentials

from pyspark.sql import SparkSession
from google.oauth2 import service_account
from google.cloud import dataproc_v1, storage

from google.cloud.dataproc_v1.services.cluster_controller.client import ClusterControllerClient
from google.cloud.dataproc_v1.services.cluster_controller.transports.grpc import ClusterControllerGrpcTransport

from google.cloud.dataproc_v1.services.job_controller.transports import JobControllerGrpcTransport
from google.cloud.dataproc_v1.services.job_controller.client import JobControllerClient
#from google.cloud.dataproc_v1.services.job_controller.transports.base import JobControllerTransport

credentials = "../../iac/credentials_project.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../../iac/credentials_project.json"

project_id   = "my-study-project-315901"
region       = "southamerica-east1"
cluster_name = "mrsa-cluster"

data_path   = "../../data/"
python_path = "../../scripts/python/"
spark_path  = "../../scripts/spark/"

script_python_folder = "scripts-python/"
script_spark_folder  = "scripts-spark/"

bucket_name = "mrsa-datalake"
bronze_zone = "bronze-zone/"
silver_zone = "silver-zone/"
gold_zone   = "gold-zone/"

def upload_data_to_gcs_bucket( bucket_path, origin_path, destination_path ):
    gcs_client = storage.Client()
    gcs_bucket = gcs_client.get_bucket(bucket_path)

    # get files to uploading
    for root, dirs, filenames in os.walk(origin_path):
        for f in filenames:
            filename = os.path.relpath(os.path.join(root, f), origin_path)
            gcs_target = gcs_bucket.blob(destination_path + filename)
            gcs_target.upload_from_filename(origin_path + filename)

def get_dataproc_processing():
    #gcp_credentials = service_account.Credentials.from_service_account_file(credentials_file)

    gdp_region_endpoint = '{}-dataproc.googleapis.com:443'.format(region)
    gdp_transport       = ClusterControllerGrpcTransport(host=gdp_region_endpoint)
    gdp_client          = dataproc_v1.ClusterControllerClient(transport=gdp_transport)

    gdp_cluster = gdp_client.get_cluster( 
                                    project_id=project_id,
                                    region=region,
                                    cluster_name=cluster_name)

    job_spark_file      = "gs://" + bucket_name + "/" + script_spark_folder + "job_spark_processing.py"
    job_transport       = JobControllerGrpcTransport(host=gdp_region_endpoint)
    job_dataproc_client = JobControllerClient(transport=job_transport)

    job_details = {
        'placement': {
            'cluster_name': cluster_name
        },
        'pyspark_job': {
            'main_python_file_uri': job_spark_file
        }
    }

    response = job_dataproc_client.submit_job(
                                           project_id=project_id,
                                           region=region,
                                           job=job_details)

    gdp_client.stop_cluster()

if __name__ == '__main__':
    print('** Starting Upload FSiles')
    upload_data_to_gcs_bucket( bucket_name, data_path, bronze_zone )
    upload_data_to_gcs_bucket( bucket_name, python_path, script_python_folder)
    upload_data_to_gcs_bucket( bucket_name, spark_path,  script_spark_folder)

    print('** Starting Dataproc Cluster and Jobs')
    get_dataproc_processing()

    print('** Stoping Dataproc Cluster')
    stop_dataproc()
