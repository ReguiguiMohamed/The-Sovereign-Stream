# Control root: the evidence teardown. It lives outside the evidence state so
# destroying the evidence stack never removes its own deadline.

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
    prefix = "control"
  }
}

variable "project_id" {
  type    = string
  default = "eventproof-stream-2609"
}

variable "region" {
  type    = string
  default = "europe-west1"
}

variable "window_end" {
  description = "RFC 3339 UTC time after which the evidence stack is destroyed."
  type        = string
  validation {
    condition     = can(timeadd(var.window_end, "0s"))
    error_message = "window_end must be an RFC 3339 timestamp."
  }
}

variable "bundle_generation" {
  description = "GCS generation of teardown/evidence.tgz, uploaded by the bundle build."
  type        = string
}

variable "bigtable_instance" {
  type = string
}

variable "alert_email" {
  type = string
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  build_account     = "cloud-build@${var.project_id}.iam.gserviceaccount.com"
  scheduler_account = "teardown-scheduler@${var.project_id}.iam.gserviceaccount.com"
  env = [
    "PROJECT_ID=${var.project_id}",
    "REGION=${var.region}",
    "WINDOW_END=${var.window_end}",
    "BIGTABLE_INSTANCE=${var.bigtable_instance}",
    "BUILD_ID=$BUILD_ID",
  ]
  gcloud    = "gcr.io/cloud-builders/gcloud"
  terraform = "hashicorp/terraform:1.16.3@sha256:c9a9d991c113f3bda5269de1506983d45ce1409dfe702df433acf10a5ea9f6bc"
  teardown_build = {
    serviceAccount = "projects/${var.project_id}/serviceAccounts/${local.build_account}"
    source = {
      storageSource = {
        bucket     = "${var.project_id}-tfstate"
        object     = "teardown/evidence.tgz"
        generation = var.bundle_generation
      }
    }
    steps = [
      { id = "gate", name = local.gcloud, entrypoint = "sh", args = ["infra/teardown.sh", "gate"], env = local.env },
      { id = "destroy", name = local.terraform, entrypoint = "sh", args = ["infra/teardown.sh", "destroy"], env = local.env },
      { id = "verify", name = local.gcloud, entrypoint = "sh", args = ["infra/teardown.sh", "verify"], env = local.env },
    ]
    logsBucket = "gs://${var.project_id}-build-source/logs"
    options    = { logging = "GCS_ONLY" }
    timeout    = "3600s"
    tags       = ["teardown"]
  }
}

# Every 15 minutes: the gate skips until window_end, then each run retries the
# destroy until verify confirms an empty inventory and pauses this job.
resource "google_cloud_scheduler_job" "teardown" {
  name      = "evidence-teardown"
  region    = var.region
  schedule  = "*/15 * * * *"
  time_zone = "Etc/UTC"
  # A verified teardown pauses this job. Reopening a window applies the control
  # root again, which resumes it; the evidence guard refuses to apply while it
  # is paused. The deadline covers submitting the build, not running it.
  paused           = false
  attempt_deadline = "180s"

  http_target {
    http_method = "POST"
    uri         = "https://cloudbuild.googleapis.com/v1/projects/${var.project_id}/locations/${var.region}/builds"
    body        = base64encode(jsonencode(local.teardown_build))
    headers     = { "Content-Type" = "application/json" }

    oauth_token {
      service_account_email = local.scheduler_account
    }
  }
}

resource "google_monitoring_notification_channel" "owner" {
  display_name = "EventProof owner"
  type         = "email"
  labels = {
    email_address = var.alert_email
  }
}

resource "google_monitoring_alert_policy" "teardown_failed" {
  display_name          = "Evidence teardown failed"
  combiner              = "OR"
  notification_channels = [google_monitoring_notification_channel.owner.id]

  # One condition: a log-based policy may hold only one. Each clause is a
  # failure no other clause sees.
  conditions {
    display_name = "teardown failed, was not dispatched, or its build did not succeed"
    condition_matched_log {
      filter = <<-EOT
        severity>=ERROR AND (
          logName="projects/${var.project_id}/logs/eventproof-teardown"
          OR (resource.type="cloud_scheduler_job" AND resource.labels.job_id="${google_cloud_scheduler_job.teardown.name}")
          OR (resource.type="build" AND operation.last=true AND protoPayload.authenticationInfo.principalEmail="${local.scheduler_account}")
        )
      EOT
    }
  }

  alert_strategy {
    notification_rate_limit {
      period = "3600s"
    }
    auto_close = "86400s"
  }
}

output "window_end" {
  value = var.window_end
}

output "bundle_generation" {
  value = var.bundle_generation
}
