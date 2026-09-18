terraform {
  required_version = "~> 1.16.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "8.2.0"
    }
  }

  backend "gcs" {
    bucket = "eventproof-stream-2609-tfstate"
    prefix = "evidence"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
