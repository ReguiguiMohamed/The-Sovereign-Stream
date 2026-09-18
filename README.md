# The Sovereign Stream

A public social-activity observatory on GCP. Live Bluesky activity flows through
Kafka and Flink into Bigtable and is served by a Cloud Run API. Code namespace:
`eventproof`. Current brief: [cloud initiation](docs/cloud-initiation.md).

```text
Bluesky Jetstream v2
  -> JetstreamProducer (GKE)
  -> Managed Service for Apache Kafka
  -> CurrentStateJob (Flink 2.2.1 on GKE, checkpoints in GCS)
  -> Bigtable current state
  -> CurrentStateApi (Cloud Run)
```

## Guarantees

1. A redelivered revision is never applied twice.
2. An older revision never replaces a newer one: Bigtable compares revisions on
   the server, so concurrency, retries, replay and expired Flink state converge
   on the newest.
3. A deleted record converges to `deleted`.
4. Records with the same id but different types stay separate.
5. A deleted account's earlier records are purged, and its records stay hidden
   while it is inactive.
6. The producer's resume cursor never passes an unpublished message: each batch
   is one Kafka transaction, and malformed messages are quarantined in it.
7. Keyed state restores from a savepoint.

Each guarantee has a test, and the
[discrimination check](tools/discrimination-check.sh) requires the named test to
report an assertion failure when the defect is put back.

## Done

- GCP project `eventproof-stream-2609` with Cloud Build bootstrap and an
  owner-run IAM script ([infra](infra/README.md)).
- `record-state.v1` contract for Bluesky record revisions and account markers
  ([streaming](streaming/README.md)).
- Transactional Jetstream producer that resumes from the last committed record.
- Flink job reading Kafka and writing Bigtable through conditional writes.
- Query API ready for Cloud Run, hiding purged and inactive-account records.
- [Cloud Build pipeline](cloudbuild.yaml): tests, shaded jar, both images, and a
  smoke test of the packaged images against live Jetstream.
- Terraform evidence root and the scheduled, verified teardown it is pinned to.
- Operator install and workload manifests for GKE ([deploy](deploy/)).
- [Jetstream sample](tools/jetstream-sample.yaml) and
  [cost estimate](docs/cost-estimate.md) from measured traffic.

## Build

```bash
gcloud builds submit --config=cloudbuild.yaml --region=europe-west1 \
  --project=eventproof-stream-2609 \
  --gcs-source-staging-dir=gs://eventproof-stream-2609-build-source/source \
  --service-account=projects/eventproof-stream-2609/serviceAccounts/cloud-build@eventproof-stream-2609.iam.gserviceaccount.com
```

## Repository map

| Path | Contents |
| --- | --- |
| [`contracts/`](contracts/) | Event schema |
| [`streaming/`](streaming/README.md) | Producer, Flink job, Bigtable contract, query API, Dockerfile |
| [`tools/`](tools/) | Cloud Build sample and discrimination check |
| [`infra/`](infra/README.md) | Bootstrap record and Terraform roots |
| [`deploy/`](deploy/) | Flink operator install and Kubernetes workloads |
| [`docs/`](docs/) | Briefs, decisions and checkpoints |

## Data

Bluesky public data through the free live Jetstream endpoint. Record bodies,
text and media are not stored. See Bluesky's
[developer guidelines](https://bsky.network/docs/developer-guidelines/).

## License

MIT for the code. See [LICENSE](LICENSE).
