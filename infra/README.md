# Infrastructure

Project `eventproof-stream-2609` (number 1082802456769), organization
`mohamed-reguigui-org`, region `europe-west1`, linked to the existing trial
billing account. Created with gcloud on 17 September 2026.

## Bootstrap

| Resource | Purpose | Access |
| --- | --- | --- |
| Service account `cloud-build` | Identity of every Cloud Build run | — |
| `gs://eventproof-stream-2609-tfstate` | Terraform state and the teardown bundle, versioned | `cloud-build`: object admin |
| `gs://eventproof-stream-2609-build-source` | Build source, logs and saved plans, 7-day lifecycle | `cloud-build`: object admin, bucket reader |
| `gs://eventproof-stream-2609-capture` | Jetstream samples, 7-day lifecycle | `cloud-build`: object admin |
| Artifact Registry `eventproof` (Docker) | `flink-job` and `service` images | `cloud-build`: writer |

All buckets use uniform access and public access prevention. Enabled APIs: Cloud
Build, Artifact Registry, Storage, IAM, Resource Manager, Service Usage, Logging,
Managed Kafka, Bigtable, GKE, Compute, Cloud Run, Billing Budgets, Cloud
Scheduler, Cloud Billing.

[`bootstrap.sh`](bootstrap.sh) creates the runtime identities, grants the
deployment identity one role per stated purpose, and creates the gross-cost
budget alerts. It is owner-run and rerunnable, so Terraform never holds
project-wide policy control.

Two of its grants are easy to get wrong and are called out in the script:
`cloud-build` needs `roles/iam.serviceAccountUser` **on** `teardown-scheduler`
to attach that identity to the Scheduler job, as well as the reverse binding
that lets the dispatcher submit builds as `cloud-build`; and the checkpoint
bucket's conditional `roles/storage.admin` must leave bucket creation granted,
since that call is authorised on the project and the project carries no bucket
`resource.name`.

The evidence root's `api_invokers` must include the `cloud-build` identity: the
deploy build calls the private API as itself to accept the deployment.

## Retained resources

Destroyed only by an explicit decision, never by the scheduled teardown:

| Resource | Condition |
| --- | --- |
| Bigtable free-trial instance | One per project, for the lifetime of the project: deleting it forfeits any further trial instance. Console-created, referenced by `bigtable_instance`. Its `current-state` table and data belong to the evidence root and go with the window. |
| Bootstrap buckets, registry, identities | Outlive the windows; cost is storage only. |

## Terraform

Both roots run only in Cloud Build through [`cloudbuild.yaml`](cloudbuild.yaml),
with state in the bootstrap bucket.

| Root | Contents |
| --- | --- |
| [`evidence`](evidence/) | VPC, Cloud NAT, private GKE cluster with one e2-standard-4 node, Managed Kafka (3 vCPU, 3 GiB) with topics `bluesky-records` and `bluesky-quarantine`, the `current-state` table in the retained instance, checkpoint bucket, Workload Identity bindings, Cloud Run API |
| [`control`](control/) | The teardown schedule, its pinned bundle and the failure alert |

## Teardown

1. `_ACTION=bundle` uploads the evidence configuration, its lock file, its
   variables and [`teardown.sh`](teardown.sh) to the versioned state bucket and
   prints the object generation.
2. The control root pins that generation and the window end, and schedules a
   build every 15 minutes.
3. An evidence apply refuses to run unless the pinned bundle matches the
   configuration being applied, at least 30 minutes of the window remain, and
   the Scheduler job that exists right now would destroy it:
   [`window_guard.py`](window_guard.py) compares the live job with the control
   configuration that declared it, down to the build body, the pinned bundle
   generation and the dispatch identity, and refuses a missing, paused or
   mismatched schedule. A teardown pauses that job when it verifies; reopening a
   window applies the control root again, which resumes it.
   [`test_window_guard.py`](test_window_guard.py) holds each refusal and runs in
   the guard step before the guard is trusted.
4. Each scheduled run skips while the window is open or another teardown is
   running, then destroys, then verifies that no cluster, Kafka cluster, Cloud
   Run service, router, network, table or checkpoint bucket remains. It retries
   every 15 minutes until that verification passes, then pauses itself.
5. Three log conditions alert the owner by e-mail, because no Google metric
   reports either a Scheduler dispatch or a build's verdict:
   - what the teardown reports itself, in log `eventproof-teardown` at ERROR;
   - a failed dispatch, on the `cloud_scheduler_job` resource at ERROR, for a
     run that never reached Cloud Build;
   - Cloud Build's own terminal audit entry for a teardown build, which covers
     the failures no step of ours can report: source fetch, a failing gate, a
     timeout. Failed build `770d4aaa-48e0-42ce-9109-8ce4cb2f9aa6` shows the
     shape this matches: `resource.type="build"`, `operation.last=true`,
     `severity=ERROR`.

   The dispatch and build conditions are verified against the live services only
   once the control root has been applied.

The bundle is complete before the first apply, so a partial apply is still
covered by a teardown that destroys exactly the configuration that was applied.
