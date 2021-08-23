from os.path import basename
from pyspark.sql import SparkSession

def format_to_parquet_and_move_zone( bucket_path, origin_path, destination_path ):
    spark = (
        SparkSession
                    .builder
                    .appName("SparkProcess")
                    .getOrCreate()
    )

    loadCensoFiles = (
        spark
            .read
            .format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .option("delimiter", "|")
            .load("gs://" + bucket_path + origin_path)
    )

    (
    loadCensoFiles
            .write
            .mode("overwrite")
            .format("parquet")
            .partitionBy("CO_UF")
            .save("gs://" + bucket_path + destination_path)
    )


if __name__ == '__main__':

    bucket_name = "mrsa-datalake/"
    bronze_zone = "bronze-zone/"
    silver_zone = "silver-zone/"

    format_to_parquet_and_move_zone(
                            bucket_path=bucket_name,
                            origin_path=bronze_zone,
                            destination_path=silver_zone)
