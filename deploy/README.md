# Deploy

[`cloudbuild.yaml`](cloudbuild.yaml) runs after an evidence apply, in three
steps.

1. `outputs` reads the Terraform outputs of the evidence root.
2. `deploy` installs Helm and the Flink Kubernetes Operator 1.15.0 chart, each
   checked against a pinned SHA-256 or SHA-512 sum, then applies
   [`workloads.yaml`](workloads.yaml) with the image digests and Terraform
   outputs filled in.
3. `accept` runs [`accept.sh`](accept.sh). A deployment counts only if this
   step passes.

## Workloads

`jetstream-producer` is a single replica with the Recreate strategy, so two
producers never overlap. It runs as non-root on a read-only root filesystem and
reaches Kafka as its own Google service account through Workload Identity.

`current-state` is a FlinkDeployment. Its TaskManager has 1 CPU, 3 GiB and two
slots, and the job runs at parallelism 2 on RocksDB, with incremental
checkpoints to Cloud Storage every 30 seconds. Kubernetes high availability
keeps the JobManager's metadata in the same bucket, and an upgrade restores from
the last state.

Measured while catching up, the TaskManager sat at 94% of its CPU limit. That
kept pace with the live firehose with little to spare, so a backlog left by a
restart drained only in Bluesky's quieter hours.

## Acceptance

`accept.sh` passes only when all of this holds, read from the systems
themselves:

1. The Flink job reports `RUNNING`.
2. A checkpoint completes in the real bucket, as reported by the JobManager's
   REST API through [`recovery_check.py`](recovery_check.py).
3. The TaskManager pod is deleted, and the job restores from that checkpoint or
   a newer one, completes another checkpoint and keeps processing records.
4. The private API refuses a call that carries no token.
5. [`record_trace.py`](record_trace.py) takes a like the pipeline stored in
   Bigtable, asks the API for it with a Google-signed identity token, and checks
   the answer against the same record fetched from Bluesky's public API.

Output of the final acceptance, build `ef901106-00d2-4bb5-84f0-a18283b40aca`
on 19 September 2026, with account ids shortened.

```text
ACCEPT Flink job RUNNING
ACCEPT checkpoint 825 completed at gs://eventproof-stream-2609-flink/checkpoints/8d7e42d3c41e37055f40011230b661ce/chk-825
ACCEPT job 8d7e42d3c41e37055f40011230b661ce restored from checkpoint 826 at gs://eventproof-stream-2609-flink/checkpoints/8d7e42d3c41e37055f40011230b661ce/chk-826
ACCEPT checkpoint 827 completed after recovery at gs://eventproof-stream-2609-flink/checkpoints/8d7e42d3c41e37055f40011230b661ce/chk-827
ACCEPT 7222 records read and written after recovery, up from 0
ACCEPT unauthenticated request rejected with 403
ACCEPT app.bsky.feed.like did:plc:2222.../3mvsux5nscx2r revision 3mvsux5o43h2r subject at://did:plc:aopn.../app.bsky.feed.post/3mvsuk6tcmc2l, confirmed by the source
```

## Reaching the private API

The service never had public access. For the dashboard, the owner ran
`gcloud run services proxy current-state-api --region=europe-west1 --port=8080`,
which forwards `127.0.0.1:8080` to the service with the caller's own identity
token attached.
