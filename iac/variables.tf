variable "project_id" {
    type        = string
    description = "Google Project ID"
    default     = "igti-projects-de"
}

variable "region" {
    type        = string
    description = "Google Cloud Region"
    default     = "southamerica-east1"
}

variable "zone" {
    type        = string
    description = "Google Cloud Zone"
    default     = "southamerica-east1-a"
}

variable "bucket_name" {
    type    = string
    default = "mrsa-datalake"
}