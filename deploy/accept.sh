#!/usr/bin/env bash
# Deployment acceptance. The managed stack is accepted only when the Flink job
# runs, checkpoints to the real bucket, comes back from losing its TaskManager,
# and the authenticated API answers with a record the source still serves.
set -euo pipefail
NS=eventproof
FLINK=flinkdeployment/current-state

out() {
  python3 -c "import json,sys; print(json.load(open('/workspace/outputs.json'))[sys.argv[1]]['value'])" "$1"
}
fail() { echo "ACCEPTANCE FAILED: $*" >&2; exit 1; }
state() { kubectl -n $NS get $FLINK -o jsonpath='{.status.jobStatus.state}'; }

BUCKET=$(out checkpoint_bucket)
gcloud container clusters get-credentials "$(out cluster)" --zone=europe-west1-b --project="$PROJECT_ID" --dns-endpoint

# The job reaches RUNNING, not merely deployed.
for attempt in $(seq 60); do
  [ "$(state)" = RUNNING ] && break
  sleep 10
done
[ "$(state)" = RUNNING ] || fail "Flink job is '$(state)', not RUNNING"
echo "ACCEPT Flink job RUNNING"

# Checkpointing and recovery, from the JobManager itself; see recovery_check.py.
kubectl -n $NS port-forward svc/current-state-rest 8081:8081 > /workspace/port-forward.log 2>&1 &
forward=$!
trap 'kill $forward 2>/dev/null || true' EXIT
for attempt in $(seq 30); do
  curl -sf http://localhost:8081/config > /dev/null 2>&1 && break
  sleep 2
done
curl -sf http://localhost:8081/config > /dev/null || fail "the Flink REST API did not answer"
python3 deploy/recovery_check.py http://localhost:8081 "$BUCKET"

# The API serves a captured record, and the source still agrees with it.
python3 deploy/record_trace.py /workspace/outputs.json
