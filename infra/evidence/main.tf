# Evidence stack: everything here is destroyed by the teardown build.

locals {
  namespace = "eventproof"
  # Created by infra/bootstrap.sh, which also grants their project roles.
  accounts = {
    for name in ["gke-nodes", "flink", "producer", "api"] :
    name => "eventproof-${name}@${var.project_id}.iam.gserviceaccount.com"
  }
  # Kubernetes service account -> Google service account, via Workload Identity.
  workloads = toset(["flink", "producer"])
}

# Network: private nodes, Cloud NAT for Jetstream and image pulls.

resource "google_compute_network" "main" {
  name                    = "eventproof"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "main" {
  name                     = "eventproof"
  region                   = var.region
  network                  = google_compute_network.main.id
  ip_cidr_range            = "10.10.0.0/20"
  private_ip_google_access = true

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.20.0.0/16"
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.30.0.0/20"
  }
}

resource "google_compute_router" "main" {
  name    = "eventproof"
  region  = var.region
  network = google_compute_network.main.id
}

resource "google_compute_router_nat" "main" {
  name                               = "eventproof"
  router                             = google_compute_router.main.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

# Workload Identity needs the cluster's identity pool, so it is bound here.

resource "google_service_account_iam_member" "workload_identity" {
  for_each           = local.workloads
  service_account_id = "projects/${var.project_id}/serviceAccounts/${local.accounts[each.key]}"
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${local.namespace}/${each.key}]"
  depends_on         = [google_container_cluster.main]
}

# GKE: one zonal cluster, one fixed node. The control plane is reached only
# through its IAM-authenticated DNS endpoint.

resource "google_container_cluster" "main" {
  name                     = "eventproof"
  location                 = var.zone
  network                  = google_compute_network.main.id
  subnetwork               = google_compute_subnetwork.main.id
  remove_default_node_pool = true
  initial_node_count       = 1
  deletion_protection      = false

  # The temporary default pool uses the node identity too.
  node_config {
    service_account = local.accounts["gke-nodes"]
  }

  release_channel {
    channel = "REGULAR"
  }

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  private_cluster_config {
    enable_private_nodes = true
  }

  control_plane_endpoints_config {
    dns_endpoint_config {
      allow_external_traffic = true
    }
    ip_endpoints_config {
      enabled = false
    }
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  logging_config {
    enable_components = ["SYSTEM_COMPONENTS", "WORKLOADS"]
  }

  monitoring_config {
    enable_components = ["SYSTEM_COMPONENTS"]
    managed_prometheus {
      enabled = true
    }
  }
}

resource "google_container_node_pool" "main" {
  name       = "main"
  cluster    = google_container_cluster.main.id
  location   = var.zone
  node_count = 1

  node_config {
    machine_type    = "e2-standard-4"
    disk_type       = "pd-balanced"
    disk_size_gb    = 50
    service_account = local.accounts["gke-nodes"]
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]

    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    shielded_instance_config {
      enable_secure_boot = true
    }
  }
}

# Kafka: the minimum cluster. Single-partition topics keep the producer's resume
# cursor in the record topic itself.

resource "google_managed_kafka_cluster" "main" {
  cluster_id = "eventproof"
  location   = var.region

  capacity_config {
    vcpu_count   = 3
    memory_bytes = 3 * 1024 * 1024 * 1024
  }

  gcp_config {
    access_config {
      network_configs {
        subnet = google_compute_subnetwork.main.id
      }
    }
  }
}

resource "google_managed_kafka_topic" "quarantine" {
  topic_id           = "bluesky-quarantine"
  cluster            = google_managed_kafka_cluster.main.cluster_id
  location           = var.region
  partition_count    = 1
  replication_factor = 3
  configs = {
    "retention.ms"   = tostring(3 * 24 * 60 * 60 * 1000)
    "cleanup.policy" = "delete"
  }
}

resource "google_managed_kafka_topic" "records" {
  topic_id           = "bluesky-records"
  cluster            = google_managed_kafka_cluster.main.cluster_id
  location           = var.region
  partition_count    = 1
  replication_factor = 3
  configs = {
    "retention.ms"   = tostring(3 * 24 * 60 * 60 * 1000)
    "cleanup.policy" = "delete"
  }
}

# Bigtable: the table lives in the retained free-trial instance and is
# destroyed with the window; the instance is not managed here.

resource "google_bigtable_table" "current_state" {
  name                = "current-state"
  instance_name       = var.bigtable_instance
  deletion_protection = "UNPROTECTED"

  column_family {
    family = "cs"
  }
}

resource "google_bigtable_gc_policy" "current_state" {
  instance_name   = var.bigtable_instance
  table           = google_bigtable_table.current_state.name
  column_family   = "cs"
  deletion_policy = "ABANDON"
  gc_rules        = jsonencode({ rules = [{ max_version = 1 }] })
}

# Flink checkpoints and savepoints.

resource "google_storage_bucket" "flink" {
  name                        = "${var.project_id}-flink"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket_iam_member" "flink" {
  bucket = google_storage_bucket.flink.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${local.accounts["flink"]}"
}

# Query API: private to the listed invokers.

resource "google_cloud_run_v2_service" "api" {
  name                = "current-state-api"
  location            = var.region
  deletion_protection = false

  template {
    service_account                  = local.accounts["api"]
    max_instance_request_concurrency = 16

    scaling {
      max_instance_count = 2
    }

    containers {
      image = var.service_image
      args  = ["dev.eventproof.streaming.CurrentStateApi"]

      env {
        name  = "BIGTABLE_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "BIGTABLE_INSTANCE_ID"
        value = var.bigtable_instance
      }
      env {
        name  = "BIGTABLE_TABLE_ID"
        value = google_bigtable_table.current_state.name
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "1Gi"
        }
      }
    }
  }
}

resource "google_cloud_run_v2_service_iam_member" "invokers" {
  for_each = toset(var.api_invokers)
  name     = google_cloud_run_v2_service.api.name
  location = var.region
  role     = "roles/run.invoker"
  member   = each.value
}
