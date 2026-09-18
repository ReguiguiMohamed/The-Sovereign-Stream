variable "project_id" {
  type    = string
  default = "eventproof-stream-2609"
}

variable "region" {
  type    = string
  default = "europe-west1"
}

variable "zone" {
  type    = string
  default = "europe-west1-b"
}

variable "service_image" {
  description = "Artifact Registry digest reference of the service image."
  type        = string
}

variable "api_invokers" {
  description = "IAM members allowed to call the query API."
  type        = list(string)
}

variable "bigtable_instance" {
  description = "Existing free-trial Bigtable instance, retained outside this root."
  type        = string
}
