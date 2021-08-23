from os import environ, path, walk
from json import load as json_load

from pyspark.sql import SparkSession

from google.oauth2 import service_account
from google.cloud import dataproc_v1, storage
from google.cloud.dataproc_v1.services.cluster_controller.client import ClusterControllerClient
from google.cloud.dataproc_v1.services.cluster_controller.transports.grpc import ClusterControllerGrpcTransport
from google.cloud.dataproc_v1.services.job_controller.transports import JobControllerGrpcTransport
from google.cloud.dataproc_v1.services.job_controller.client import JobControllerClient


def upload_data_to_bucket( bucket_path, origin_path, destination_path ):
    gcs_client = storage.Client()
    gcs_bucket = gcs_client.get_bucket(bucket_path)

    # get files to uploading
    for root, dirs, filenames in walk(origin_path):
        for f in filenames:
            filename = path.relpath(path.join(root, f), origin_path)
            gcs_target = gcs_bucket.blob(destination_path + filename)
            gcs_target.upload_from_filename(origin_path + filename)


def get_dataproc_processing( bucket_name, project, region, cluster_name, spark_folder):
    print('*** Starting Dataproc Cluster and Jobs ***')
    gdp_region_endpoint  = '{}-dataproc.googleapis.com:443'.format(region)
    gdp_transport_client = ClusterControllerGrpcTransport(host=gdp_region_endpoint)
    gdp_cluster_client   = dataproc_v1.ClusterControllerClient(transport=gdp_transport_client)

    gdp_cluster = gdp_cluster_client.get_cluster(
                                            project_id=project,
                                            region=region,
                                            cluster_name=cluster_name)

    job_spark_uri       = "gs://" + bucket_name + "/" + spark_folder + "job_spark_processing.py"
    job_transport       = JobControllerGrpcTransport(host=gdp_region_endpoint)
    job_dataproc_client = JobControllerClient(transport=job_transport)

    job_details = {
        'placement': {
            'cluster_name': cluster_name
        },
        'pyspark_job': {
            'main_python_file_uri': job_spark_uri
        }
    }

    result = job_dataproc_client.submit_job(
                                        project_id=project,
                                        region=region,
                                        job=job_details)

if __name__ == '__main__':

    # get parameters
    with open('../../static/parameters.json') as pars:
        params = json_load(pars)

    environ["GOOGLE_APPLICATION_CREDENTIALS"] = params["CREDENTIALS"]

    print('*** Starting Upload Files ***')
    upload_data_to_bucket( bucket_path=params["BUCKET_NAME"], origin_path=params["DATA_PATH"], destination_path=params["BRONZE_ZONE"] )
    upload_data_to_bucket( bucket_path=params["BUCKET_NAME"], origin_path=params["PYTHON_PATH"], destination_path=params["PYTHON_SCRIPT_FOLDER"] )
    upload_data_to_bucket( bucket_path=params["BUCKET_NAME"], origin_path=params["SPARK_PATH"],  destination_path=params["SPARK_SCRIPT_FOLDER"] )

    get_dataproc_processing( bucket_name=params["BUCKET_NAME"],
                             project=params["PROJECT_ID"],
                             region=params["REGION"],
                             cluster_name=params["CLUSTER_NAME"],
                             spark_folder=params["SPARK_SCRIPT_FOLDER"] )

