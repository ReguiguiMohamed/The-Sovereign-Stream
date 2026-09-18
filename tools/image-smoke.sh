#!/usr/bin/env bash
# Runs the packaged images end to end inside Cloud Build, against live Jetstream:
#   service image (producer) -> Kafka broker -> flink-job image (application mode)
#   -> Bigtable emulator -> service image (API)
# then restarts the producer and checks it resumes from the last committed record.
# Needs FLINK_IMAGE and SERVICE_IMAGE built locally by the same build.
set -euo pipefail
: "${FLINK_IMAGE:?}" "${SERVICE_IMAGE:?}"
KAFKA=apache/kafka:4.2.0@sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9
EMULATOR=gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators@sha256:3294e8a543de846703594a8a89bfe009e0e9ffbdd2e495d00133f3de14cabcc0
CURL=curlimages/curl:8.16.0@sha256:463eaf6072688fe96ac64fa623fe73e1dbe25d8ad6c34404a669ad3ce1f104b6
NET=cloudbuild
TOPIC=bluesky-records
QUARANTINE=bluesky-quarantine
CONTAINERS="producer flink-tm flink-jm api bigtable kafka"

cleanup() {
  for c in $CONTAINERS; do
    docker logs "$c" 2>&1 | grep -iE "exception|caused by|at dev\.eventproof|error" | head -30       | sed "s/^/[$c!] /" || true
    docker logs --tail 15 "$c" 2>&1 | sed "s/^/[$c] /" || true
    docker rm -f "$c" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

fail() { echo "SMOKE FAILED: $*"; exit 1; }

kafka() { docker exec kafka /opt/kafka/bin/"$@"; }

# Highest committed source_seq in a topic, or -1.
max_seq() {
  kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "$1" \
    --from-beginning --isolation-level read_committed --timeout-ms 15000 2>/dev/null \
    | grep -o '"source_seq":[0-9]*' | cut -d: -f2 | sort -n | tail -1 | grep . || echo -1
}

docker run -d --name kafka --network $NET \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  $KAFKA >/dev/null
docker run -d --name bigtable --network $NET $EMULATOR \
  gcloud beta emulators bigtable start --host-port=0.0.0.0:8086 >/dev/null

for attempt in $(seq 60); do
  kafka kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && break
  sleep 2
done
for topic in $TOPIC $QUARANTINE; do
  kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $topic --partitions 1
done
# The emulators image ships the emulator but not cbt.
docker exec bigtable gcloud components install cbt --quiet >/dev/null
docker exec -e BIGTABLE_EMULATOR_HOST=localhost:8086 bigtable \
  cbt -project smoke -instance smoke createtable current-state families=cs:maxversions=1
docker exec -e BIGTABLE_EMULATOR_HOST=localhost:8086 bigtable \
  cbt -project smoke -instance smoke ls | grep -qx current-state || fail "no emulator table"

common=(--network $NET
  -e KAFKA_BOOTSTRAP=kafka:9092 -e KAFKA_AUTH=none
  -e KAFKA_TOPIC=$TOPIC -e KAFKA_QUARANTINE_TOPIC=$QUARANTINE
  -e BIGTABLE_PROJECT_ID=smoke -e BIGTABLE_INSTANCE_ID=smoke
  -e BIGTABLE_TABLE_ID=current-state -e BIGTABLE_EMULATOR_HOST=bigtable:8086)
# A directory both containers share: the job's state outgrows memory checkpoints.
mkdir -p /workspace/checkpoints && chmod 777 /workspace/checkpoints
flink=(-v /workspace/checkpoints:/checkpoints
  -e "FLINK_PROPERTIES=jobmanager.rpc.address: flink-jm
taskmanager.numberOfTaskSlots: 2
parallelism.default: 2
execution.checkpointing.interval: 10s
execution.checkpointing.dir: file:///checkpoints")

docker run -d --name producer "${common[@]}" "$SERVICE_IMAGE" \
  dev.eventproof.streaming.JetstreamProducer >/dev/null
docker run -d --name flink-jm "${common[@]}" "${flink[@]}" "$FLINK_IMAGE" \
  standalone-job --job-classname dev.eventproof.streaming.CurrentStateJob >/dev/null
docker run -d --name flink-tm "${common[@]}" "${flink[@]}" "$FLINK_IMAGE" taskmanager >/dev/null
docker run -d --name api "${common[@]}" "$SERVICE_IMAGE" \
  dev.eventproof.streaming.CurrentStateApi >/dev/null

sleep 150

stats=$(docker logs producer 2>/dev/null | grep '"published"' | tail -1)
published=$(echo "$stats" | grep -o '"published":[0-9]*' | cut -d: -f2)
[ "${published:-0}" -gt 0 ] || fail "producer published nothing: $stats"
echo "SMOKE producer stats: $stats"

# The API serves records that the Flink job wrote.
served=0
for line in $(kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $TOPIC \
    --from-beginning --isolation-level read_committed --max-messages 300 --timeout-ms 15000 \
    2>/dev/null | grep '"entity_type":"app.bsky.feed.post"' \
    | sed 's/.*"entity_id":"\([^"]*\)".*/\1/' | head -20); do
  status=$(docker run --rm --network $NET $CURL -s -o /dev/null -w '%{http_code}' \
    "http://api:8080/v1/current-state?entity_type=app.bsky.feed.post&entity_id=$line")
  [ "$status" = 200 ] && served=$((served + 1))
done
[ "$served" -gt 0 ] || fail "API served none of the sampled posts"
echo "SMOKE API served $served of the first sampled posts"

# Restart: the resume cursor equals the highest committed sequence.
docker stop -t 20 producer >/dev/null
committed=$(max_seq $TOPIC)
[ "$committed" -ge 0 ] || fail "no committed records"
# What the topic holds against what the producer counted as committed, by
# identity as well as by count, so a duplicate cannot hide a missing record.
# The producer prints its final counters on SIGTERM; the count may exceed them
# only by a transaction committed after that print.
published=$(docker logs producer 2>/dev/null | grep -o '"published":[0-9]*' | tail -1 | cut -d: -f2)
kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $TOPIC --from-beginning --isolation-level read_committed --timeout-ms 20000 > /workspace/records.json 2>/dev/null || true
stored=$(wc -l < /workspace/records.json)
distinct=$(grep -o '"event_id":"[0-9a-f]*"' /workspace/records.json | sort -u | wc -l)
[ "$distinct" -eq "$stored" ] || fail "$stored records carry $distinct distinct event ids: the topic holds duplicates"
[ "$stored" -ge "${published:-1}" ] || fail "topic holds $stored records of the ${published:-0} the producer committed: records are missing"
echo "SMOKE topic holds $stored records with $distinct distinct identities, for $published committed by the producer"
docker start producer >/dev/null
sleep 45
resumed=$(docker logs producer 2>/dev/null | grep -o '"resume_cursor":-\?[0-9]*' | tail -1 | cut -d: -f2)
[ "$resumed" = "$committed" ] || fail "resumed from $resumed, committed $committed"
# Measuring needs an idle topic: a running producer keeps the consumer reading.
docker stop -t 20 producer >/dev/null
after=$(max_seq $TOPIC)
[ "$after" -gt "$committed" ] || fail "no progress after restart ($after <= $committed)"
echo "SMOKE restart resumed from committed seq $committed; now at $after"

quarantined=$(kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $QUARANTINE \
  --from-beginning --isolation-level read_committed --timeout-ms 10000 2>/dev/null | wc -l)
echo "SMOKE quarantined messages: $quarantined"
kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $QUARANTINE \
  --from-beginning --isolation-level read_committed --max-messages 2 --timeout-ms 10000 \
  2>/dev/null | cut -c1-160 | sed 's/^/SMOKE quarantined sample: /' || true
checkpoints=$(docker logs flink-jm 2>&1 | grep -c "Completed checkpoint" || true)
[ "$checkpoints" -gt 0 ] || fail "no completed Flink checkpoint"
echo "SMOKE Flink completed checkpoints: $checkpoints"
echo "SMOKE PASSED"
