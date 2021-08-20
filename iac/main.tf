provider "google" {
    credentials = "${file("credentials_project.json")}"
    project     = "${var.project_id}"
    region      = "${var.region}"
    zone        = "${var.zone}"
}

resource "google_dataproc_cluster" "mrsa_cluster" {
  name   = "mrsa-cluster"
  region = "${var.region}"
}

resource "google_storage_bucket" "mrsa_datalake_bucket" {
    name          = "${var.bucket_name}"
    project	      = "${var.project_id}"
    location      = "${var.region}"
    force_destroy = true
    storage_class = "STANDARD"
}

resource "google_storage_bucket_object" "bronze_folder" {
    name     = "bronze-zone/"
    content  = "bronze - raw zone"
    bucket   = google_storage_bucket.mrsa_datalake_bucket.id
}

resource "google_storage_bucket_object" "silver_folder" {
    name     = "silver-zone/"
    content  = "silver - stage zone"
    bucket   = google_storage_bucket.mrsa_datalake_bucket.id
}

resource "google_storage_bucket_object" "gold_folder" {
    name     = "gold-zone/"
    content  = "gold - curated zone"
    bucket   = "${google_storage_bucket.mrsa_datalake_bucket.name}"
}

resource "google_storage_bucket_object" "scripts_python" {
    name     = "scripts-python/"
    content  = "scripts python"
    bucket   = google_storage_bucket.mrsa_datalake_bucket.id
}

resource "google_storage_bucket_object" "scripts_spark" {
    name     = "scripts-spark/"
    content  = "scripts spark"
    bucket   = google_storage_bucket.mrsa_datalake_bucket.id
}
