from pyspark.sql import SparkSession

project_id   = "my-study-project-315901"
region       = "southamerica-east1"
cluster_name = "mrsa-cluster"

bucket_name = "mrsa-datalake"
bronze_zone = "bronze-zone/"
silver_zone = "silver-zone/"
gold_zone   = "gold-zone/"

def format_to_parquet_and_move_zone( bucket_path, origin_path, destination_path ):
    spark = (
        SparkSession.builder.appName("SparkProcess").getOrCreate()
    )

    loadFiles = (
        spark
            .read
            .format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .option("delimiter", "|")
            .load("gs://" + bucket_path + "/" + origin_path )
    )

    (
    loadFiles
        .write
        .mode("overwrite")
        .format("parquet")
        .partitionBy("nu_ano_censo")
        .save("gs://" + bucket_path + "/" + destination_path)
    )

if __name__ == '__main__':
    format_to_parquet_and_move_zone( bucket_name, bronze_zone, silver_zone )