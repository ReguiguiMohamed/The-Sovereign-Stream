# Operational overview, in Cloud Monitoring's own dashboard format: a mosaic of
# xyChart widgets, as the official samples build them.
# https://github.com/GoogleCloudPlatform/monitoring-dashboard-samples
#
# Every metric below was confirmed to carry points in this project before it was
# charted. Cloud Scheduler publishes no metrics at all, which is why the teardown
# is watched by a log-based alert policy rather than from here.

locals {
  kafka_topic = "resource.type=\"managedkafka.googleapis.com/Topic\" resource.label.\"cluster_id\"=\"${google_managed_kafka_cluster.main.cluster_id}\""

  # One minute of alignment everywhere, so the tiles read against each other.
  rate_by = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_RATE", crossSeriesReducer = "REDUCE_SUM" }
}

resource "google_monitoring_dashboard" "overview" {
  dashboard_json = jsonencode({
    displayName = "EventProof — pipeline overview"
    mosaicLayout = {
      columns = 12
      tiles = [
        {
          xPos = 0, yPos = 0, width = 6, height = 4
          widget = {
            title = "Kafka: messages accepted per second, by topic"
            xyChart = {
              dataSets = [{
                plotType   = "LINE"
                targetAxis = "Y1"
                timeSeriesQuery = { timeSeriesFilter = {
                  filter      = "metric.type=\"managedkafka.googleapis.com/message_in_count\" ${local.kafka_topic} resource.label.\"topic_id\"=starts_with(\"bluesky\")"
                  aggregation = merge(local.rate_by, { groupByFields = ["resource.label.\"topic_id\""] })
                } }
              }]
              yAxis = { label = "messages/s", scale = "LINEAR" }
            }
          }
        },
        {
          xPos = 6, yPos = 0, width = 6, height = 4
          widget = {
            title = "Kafka: records the Flink job has not consumed"
            xyChart = {
              dataSets = [{
                plotType   = "LINE"
                targetAxis = "Y1"
                timeSeriesQuery = { timeSeriesFilter = {
                  filter = "metric.type=\"managedkafka.googleapis.com/offset_lag\" resource.type=\"managedkafka.googleapis.com/TopicPartitionConsumerGroup\" resource.label.\"consumer_group_id\"=\"current-state\" resource.label.\"topic_id\"=\"${google_managed_kafka_topic.records.topic_id}\""
                  aggregation = {
                    alignmentPeriod    = "60s"
                    perSeriesAligner   = "ALIGN_MEAN"
                    crossSeriesReducer = "REDUCE_SUM"
                  }
                } }
              }]
              yAxis = { label = "records behind", scale = "LINEAR" }
            }
          }
        },
        {
          xPos = 0, yPos = 4, width = 6, height = 4
          widget = {
            title = "Bigtable: requests per second on current-state, by method"
            xyChart = {
              dataSets = [{
                plotType   = "LINE"
                targetAxis = "Y1"
                timeSeriesQuery = { timeSeriesFilter = {
                  filter      = "metric.type=\"bigtable.googleapis.com/server/request_count\" resource.type=\"bigtable_table\" resource.label.\"instance\"=\"${var.bigtable_instance}\" resource.label.\"table\"=\"${google_bigtable_table.current_state.name}\""
                  aggregation = merge(local.rate_by, { groupByFields = ["metric.label.\"method\""] })
                } }
              }]
              yAxis = { label = "requests/s", scale = "LINEAR" }
            }
          }
        },
        {
          xPos = 6, yPos = 4, width = 6, height = 4
          widget = {
            title = "Bigtable: server latency, 99th percentile"
            xyChart = {
              dataSets = [{
                plotType   = "LINE"
                targetAxis = "Y1"
                timeSeriesQuery = { timeSeriesFilter = {
                  filter = "metric.type=\"bigtable.googleapis.com/server/latencies\" resource.type=\"bigtable_table\" resource.label.\"instance\"=\"${var.bigtable_instance}\" resource.label.\"table\"=\"${google_bigtable_table.current_state.name}\""
                  aggregation = {
                    alignmentPeriod    = "60s"
                    perSeriesAligner   = "ALIGN_PERCENTILE_99"
                    crossSeriesReducer = "REDUCE_MAX"
                  }
                } }
              }]
              yAxis = { label = "ms", scale = "LINEAR" }
            }
          }
        },
        {
          xPos = 0, yPos = 8, width = 6, height = 4
          widget = {
            title = "Query API: requests per second, by response code class"
            xyChart = {
              dataSets = [{
                plotType   = "LINE"
                targetAxis = "Y1"
                timeSeriesQuery = { timeSeriesFilter = {
                  filter      = "metric.type=\"run.googleapis.com/request_count\" resource.type=\"cloud_run_revision\" resource.label.\"service_name\"=\"${google_cloud_run_v2_service.api.name}\""
                  aggregation = merge(local.rate_by, { groupByFields = ["metric.label.\"response_code_class\""] })
                } }
              }]
              yAxis = { label = "requests/s", scale = "LINEAR" }
            }
          }
        },
        {
          xPos = 6, yPos = 8, width = 6, height = 4
          widget = {
            title = "GKE: container restarts in the eventproof namespace"
            xyChart = {
              dataSets = [{
                plotType   = "LINE"
                targetAxis = "Y1"
                timeSeriesQuery = { timeSeriesFilter = {
                  filter      = "metric.type=\"kubernetes.io/container/restart_count\" resource.type=\"k8s_container\" resource.label.\"cluster_name\"=\"${google_container_cluster.main.name}\" resource.label.\"namespace_name\"=\"${local.namespace}\""
                  aggregation = merge(local.rate_by, { groupByFields = ["resource.label.\"pod_name\""] })
                } }
              }]
              yAxis = { label = "restarts/s", scale = "LINEAR" }
            }
          }
        },
      ]
    }
  })
}

output "monitoring_dashboard" {
  value = "https://console.cloud.google.com/monitoring/dashboards/builder/${basename(google_monitoring_dashboard.overview.id)}?project=${var.project_id}"
}
