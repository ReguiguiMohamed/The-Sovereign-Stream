output "kafka_bootstrap" {
  value = "bootstrap.${google_managed_kafka_cluster.main.cluster_id}.${var.region}.managedkafka.${var.project_id}.cloud.goog:9092"
}

output "kafka_topic" {
  value = google_managed_kafka_topic.records.topic_id
}

output "kafka_quarantine_topic" {
  value = google_managed_kafka_topic.quarantine.topic_id
}

output "bigtable_instance" {
  value = var.bigtable_instance
}

output "bigtable_table" {
  value = google_bigtable_table.current_state.name
}

output "checkpoint_bucket" {
  value = google_storage_bucket.flink.url
}

output "cluster" {
  value = google_container_cluster.main.name
}

output "api_url" {
  value = google_cloud_run_v2_service.api.uri
}

output "service_accounts" {
  value = local.accounts
}
