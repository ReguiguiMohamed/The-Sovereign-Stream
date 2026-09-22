# Infrastructure

Everything ran in one Google Cloud project, `eventproof-stream-2609`, in
`europe-west1`, on free-trial credit. It comes in three layers: a bootstrap
made once with gcloud, two Terraform roots that only Cloud Build applies, and a
scheduled teardown. The estimate behind the sizing is in
[docs/cost-estimate.md](../docs/cost-estimate.md).

## Bootstrap

Created with gcloud on 17 and 18 September 2026.

| Resource | Purpose |
| --- | --- |
| Service account `cloud-build` | the identity of every build |
| `gs://eventproof-stream-2609-tfstate` | Terraform state and the teardown bundle, versioned |
| `gs://eventproof-stream-2609-build-source` | build sources, logs and saved plans, deleted after 7 days |
| `gs://eventproof-stream-2609-capture` | Jetstream samples, deleted after 7 days |
| Artifact Registry `eventproof` | the `flink-job` and `service` images |
| Bigtable instance `eventproof` | the one free-trial instance a project gets, one SSD node in `europe-west1-b`, created in the Console |

Every bucket uses uniform access with public access prevention.

[`bootstrap.sh`](bootstrap.sh) is run by the project owner and is safe to
rerun. It creates the runtime identities, gives each one only the roles its job
needs, and sets two budget alerts on gross cost with credits excluded, one for
the project and one for the whole billing account. Terraform never holds
project-wide IAM control.

Two of its grants are easy to get wrong, and the script says why next to each.
`cloud-build` needs `roles/iam.serviceAccountUser` on `teardown-scheduler` to
attach that identity to the Scheduler job. And the condition that limits
`roles/storage.admin` to the checkpoint bucket has to leave bucket creation
allowed, because that call is authorised on the project, where there is no
bucket name to match.

## Terraform

Both roots run only in Cloud Build, through [`cloudbuild.yaml`](cloudbuild.yaml),
with state in the bootstrap bucket. Terraform is 1.16 and the Google provider
8.2.0, pinned by the lock files. `_ACTION` picks what a build does: `bundle`,
`plan`, `apply` or `destroy`.

| Root | Creates |
| --- | --- |
| [`evidence`](evidence/) | VPC and Cloud NAT, a private GKE cluster with one e2-standard-4 node, Managed Kafka (3 vCPU, 3 GiB) with the `bluesky-records` and `bluesky-quarantine` topics, the `current-state` table in the free-trial instance, the checkpoint bucket, Workload Identity bindings, the private Cloud Run service and the [Monitoring dashboard](evidence/monitoring.tf) |
| [`control`](control/) | the teardown schedule, the bundle it runs, and the alert if it fails |

The GKE control plane has no IP endpoint. It is reached only through its
IAM-authenticated DNS endpoint.

## Teardown

The evidence stack was built to be destroyed on a deadline, with nobody
watching.

1. `_ACTION=bundle` uploads the evidence configuration, its lock file and
   [`teardown.sh`](teardown.sh) to the versioned state bucket and prints the
   object's generation.
2. The control root pins that generation and the window end, and schedules a
   teardown build every 15 minutes.
3. An evidence apply refuses to run unless the live Scheduler job would destroy
   exactly what it is about to create. [`window_guard.py`](window_guard.py)
   compares the job with the control configuration down to the build body, the
   bundle generation, the HTTP method and the dispatch identity. It refuses a
   missing, paused or mismatched job, and a window with less than 30 minutes
   left. [`test_window_guard.py`](test_window_guard.py) holds every refusal and
   runs before the guard is trusted.
4. Each scheduled build skips while the window is open or while another
   teardown runs. After the deadline it destroys the stack, checks that no
   cluster, Kafka cluster, Cloud Run service, router, network, table or
   checkpoint bucket is left, and pauses its own schedule once that list comes
   back empty. Until then it retries every 15 minutes.
5. One log-based alert emails the owner when the teardown logs an error, when
   Scheduler fails to dispatch it, or when a teardown build fails for any other
   reason, a failing gate or a timeout included.

The bundle exists before the first apply, so even a half-finished apply is
covered by a teardown of exactly the configuration that was applied.

## How the window ended

The window opened on 18 September 2026 with a deadline of 12:00 UTC on
21 September. No API reports the hour a free trial ends, so that deadline was an
estimate. The trial ended first, that morning. The last gate check ran at
08:15 UTC in build `41e4dcd5` and skipped as designed, then the billing account
closed and Google suspended the project with the stack inside it. A closed trial
account can't be charged, so it cost nothing further.
