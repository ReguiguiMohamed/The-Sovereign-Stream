#!/usr/bin/env bash
# Owner bootstrap for eventproof-stream-2609: identities, their IAM, and budget
# alerts. Run by the project owner; safe to rerun. The deployment identity
# (cloud-build) gets only what the Terraform roots and deploy build use; runtime
# identities get only what their workloads use.
#
#   BIGTABLE_INSTANCE=eventproof bash infra/bootstrap.sh
set -euo pipefail
P=eventproof-stream-2609
REGION=europe-west1
# Billing budgets are charged to the caller's own project, not to --project, so
# the script runs against this one whatever gcloud is configured for.
export CLOUDSDK_CORE_PROJECT=$P
BILLING_ACCOUNT=012271-AC40A5-6D1C29
: "${BIGTABLE_INSTANCE:?the free-trial Bigtable instance id}"
BUILD=cloud-build@$P.iam.gserviceaccount.com

account() { echo "$1@$P.iam.gserviceaccount.com"; }

ensure_account() {
  gcloud iam service-accounts describe "$(account "$1")" --project=$P >/dev/null 2>&1 \
    || gcloud iam service-accounts create "$1" --project=$P --display-name="$2"
}

project_role() {
  gcloud projects add-iam-policy-binding $P --member="serviceAccount:$1" --role="$2" \
    --condition=None --format=none
}

account_role() {
  gcloud iam service-accounts add-iam-policy-binding "$1" --project=$P \
    --member="serviceAccount:$2" --role="$3" --format=none
}

ensure_account eventproof-gke-nodes "GKE nodes"
ensure_account eventproof-flink "Flink job"
ensure_account eventproof-producer "Jetstream producer"
ensure_account eventproof-api "Query API"
ensure_account teardown-scheduler "Scheduled teardown dispatcher"

# Runtime identities.
project_role "$(account eventproof-gke-nodes)" roles/container.defaultNodeServiceAccount
project_role "$(account eventproof-flink)" roles/managedkafka.client
project_role "$(account eventproof-producer)" roles/managedkafka.client
gcloud artifacts repositories add-iam-policy-binding eventproof --project=$P --location=$REGION \
  --member="serviceAccount:$(account eventproof-gke-nodes)" --role=roles/artifactregistry.reader \
  --format=none
gcloud bigtable instances add-iam-policy-binding "$BIGTABLE_INSTANCE" --project=$P \
  --member="serviceAccount:$(account eventproof-flink)" --role=roles/bigtable.user --format=none
gcloud bigtable instances add-iam-policy-binding "$BIGTABLE_INSTANCE" --project=$P \
  --member="serviceAccount:$(account eventproof-api)" --role=roles/bigtable.reader --format=none

# Teardown dispatcher: submits the teardown build, which runs as cloud-build.
project_role "$(account teardown-scheduler)" roles/cloudbuild.builds.editor
account_role "$BUILD" "$(account teardown-scheduler)" roles/iam.serviceAccountUser
# Attaching that identity to the Scheduler job needs actAs on it, for the
# identity that creates the job:
# https://cloud.google.com/scheduler/docs/http-target-auth
account_role "$(account teardown-scheduler)" $BUILD roles/iam.serviceAccountUser

# Calling the private API needs an identity token, which Cloud Build's metadata
# server does not serve, so the build mints one for itself:
# https://cloud.google.com/iam/docs/create-short-lived-credentials-direct
account_role "$BUILD" $BUILD roles/iam.serviceAccountTokenCreator

# Deployment identity, project level, one purpose each.
project_role $BUILD roles/compute.networkAdmin                  # VPC, subnet, router, NAT
project_role $BUILD roles/container.admin                       # cluster, node pool, in-cluster deploy
project_role $BUILD roles/managedkafka.admin                    # Kafka cluster and topics
project_role $BUILD roles/run.admin                             # API service and its invoker binding
project_role $BUILD roles/cloudscheduler.admin                  # teardown job; pause after cleanup
project_role $BUILD roles/cloudbuild.builds.viewer              # teardown overlap check
project_role $BUILD roles/monitoring.alertPolicyEditor          # teardown failure alert
project_role $BUILD roles/monitoring.notificationChannelEditor  # alert e-mail channel
project_role $BUILD roles/logging.configWriter                  # log-based alert condition
# Checkpoint bucket only. Creating a bucket is authorised on the project, which
# provides no bucket resource.name, so a bare startsWith would deny the create.
# The condition therefore restricts buckets and objects and leaves the rest of
# the role, the shape the attribute reference prescribes.
gcloud projects add-iam-policy-binding $P --member="serviceAccount:$BUILD" \
  --role=roles/storage.admin --format=none \
  --condition="title=flink-bucket-only,expression=(resource.type != 'storage.googleapis.com/Bucket' && resource.type != 'storage.googleapis.com/Object') || resource.name.startsWith('projects/_/buckets/$P-flink')"

# Deployment identity, resource level.
gcloud bigtable instances add-iam-policy-binding "$BIGTABLE_INSTANCE" --project=$P \
  --member="serviceAccount:$BUILD" --role=roles/bigtable.admin --format=none   # the evidence table
account_role "$(account eventproof-gke-nodes)" $BUILD roles/iam.serviceAccountUser  # node pool
account_role "$(account eventproof-api)" $BUILD roles/iam.serviceAccountUser        # Cloud Run
account_role "$(account eventproof-flink)" $BUILD roles/iam.serviceAccountAdmin     # Workload Identity binding
account_role "$(account eventproof-producer)" $BUILD roles/iam.serviceAccountAdmin  # Workload Identity binding

# Budget alerts on gross cost, before trial credits, sent to the billing
# account's admins: the whole account (every project spends the same credit)
# and this project.
for scope in account project; do
  name="eventproof-gross-$scope"
  if gcloud billing budgets list --billing-account=$BILLING_ACCOUNT \
      --format="value(displayName)" | grep -qx "$name"; then
    continue
  fi
  filter=()
  [ "$scope" = project ] && filter=(--filter-projects="projects/$P")
  gcloud billing budgets create --billing-account=$BILLING_ACCOUNT \
    --display-name="$name" --budget-amount=150USD "${filter[@]}" \
    --credit-types-treatment=exclude-all-credits \
    --threshold-rule=percent=0.25 --threshold-rule=percent=0.5 \
    --threshold-rule=percent=0.8 --threshold-rule=percent=1.0
done
