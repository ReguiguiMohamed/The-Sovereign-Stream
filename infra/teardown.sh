#!/bin/sh
# Scheduled evidence teardown, run from the pinned bundle by the build that
# infra/control defines. Phases, one per build step:
#   gate     skip before WINDOW_END, or while another teardown build runs
#   destroy  terraform destroy on the evidence root
#   verify   confirm nothing from the evidence root remains, then pause the schedule
# Every failure is logged at ERROR to log eventproof-teardown, which alerts.
set -eu
phase=$1

report() {
  gcloud logging write eventproof-teardown \
    "{\"build\":\"$BUILD_ID\",\"phase\":\"$phase\",\"message\":\"$2\"}" \
    --payload-type=json --severity="$1" --project="$PROJECT_ID"
}

case "$phase" in
gate)
  if [ "$(date -u +%s)" -lt "$(date -u -d "$WINDOW_END" +%s)" ]; then
    echo "window open until $WINDOW_END"
    touch /workspace/skip
    exit 0
  fi
  running=$(gcloud builds list --project="$PROJECT_ID" --region="$REGION" \
    --filter="tags=teardown AND status=WORKING AND id!=$BUILD_ID" --format="value(id)")
  if [ -n "$running" ]; then
    echo "teardown already running: $running"
    touch /workspace/skip
  fi
  ;;
destroy)
  [ -f /workspace/skip ] && exit 0
  cd infra/evidence
  terraform init -input=false -no-color \
    && terraform destroy -auto-approve -input=false -no-color -lock-timeout=5m \
    || touch /workspace/failed
  ;;
verify)
  [ -f /workspace/skip ] && exit 0
  if [ -f /workspace/failed ]; then
    report ERROR "terraform destroy failed"
    exit 1
  fi
  remaining=""
  inventory() {
    found=$("$@") || { report ERROR "inventory failed: $*"; exit 1; }
    remaining="$remaining $found"
  }
  inventory gcloud container clusters list --project="$PROJECT_ID" --format="value(name)"
  inventory gcloud managed-kafka clusters list --project="$PROJECT_ID" --location="$REGION"     --format="value(name)"
  inventory gcloud run services list --project="$PROJECT_ID" --region="$REGION"     --format="value(metadata.name)"
  inventory gcloud compute routers list --project="$PROJECT_ID" --filter="name=eventproof"     --format="value(name)"
  inventory gcloud compute networks list --project="$PROJECT_ID" --filter="name=eventproof"     --format="value(name)"
  inventory gcloud bigtable instances tables list --project="$PROJECT_ID"     --instances="$BIGTABLE_INSTANCE" --format="value(name)"
  if gcloud storage buckets describe "gs://$PROJECT_ID-flink" >/dev/null 2>/tmp/bucket; then
    remaining="$remaining gs://$PROJECT_ID-flink"
  elif ! grep -qi "not found\|404" /tmp/bucket; then
    report ERROR "cannot check the checkpoint bucket"
    exit 1
  fi
  if [ -n "$(echo "$remaining" | tr -d '[:space:]')" ]; then
    report ERROR "resources remain: $(echo $remaining)"
    exit 1
  fi
  gcloud scheduler jobs pause evidence-teardown --project="$PROJECT_ID" --location="$REGION"
  report NOTICE "teardown verified; schedule paused"
  ;;
*)
  echo "unknown phase $phase" >&2
  exit 2
  ;;
esac
