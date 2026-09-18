#!/usr/bin/env bash
# Reintroduces each guarded defect and requires every named test to report a
# JUnit assertion failure. Any other outcome, including a build or launch error,
# fails the check.
set -euo pipefail
cd "$(dirname "$0")/../streaming"
main=src/main/java/dev/eventproof/streaming

check() {
  local name=$1 file=$2 from=$3 to=$4 class=$5
  shift 5
  cp "$file" /tmp/original
  FROM="$from" TO="$to" perl -0pi -e 's/\Q$ENV{FROM}\E/$ENV{TO}/' "$file"
  if cmp -s "$file" /tmp/original; then
    echo "MUTANT NOT APPLIED: $name"; exit 1
  fi
  rm -rf target/surefire-reports
  local methods
  methods=$(IFS=+; echo "$*")
  if mvn -q --batch-mode --no-transfer-progress test \
      -Dtest="$class#$methods" -Dsurefire.failIfNoSpecifiedTests=false > /tmp/out 2>&1; then
    echo "SURVIVED: $name"; exit 1
  fi
  local report=target/surefire-reports/TEST-dev.eventproof.streaming.$class.xml
  for method in "$@"; do
    if ! grep -A2 "<testcase name=\"$method\"" "$report" 2>/dev/null | grep -q "<failure"; then
      echo "NO ASSERTION FAILURE: $name ($class#$method)"; tail -40 /tmp/out; exit 1
    fi
  done
  echo "KILLED: $name ($class#$methods)"
  cp /tmp/original "$file"
}

check "state keyed by entity_id only" $main/CurrentStateJob.java \
  ".keyBy(CurrentStateRow::rowKey)" ".keyBy(event -> event.entityId)" \
  CurrentStateJobTest sameIdUnderAnotherTypeIsAnotherRecord
check "arrival order wins" $main/PreserveNewestState.java \
  "event.revision.compareTo(current) > 0" "true" \
  CurrentStateJobTest olderRevisionArrivingLastCannotReplaceNewer redeliveredRevisionProducesOneUpdate
check "no stable state uid" $main/CurrentStateJob.java \
  ".uid(STATE_UID)" "" \
  SavepointRecoveryTest keyedStateSurvivesStopWithSavepoint
check "unconditional storage write" $main/CurrentStateRow.java \
  ".condition(atLeast(REVISION, event.revision))" ".condition(FILTERS.block())" \
  BigtableCurrentStateTest sameMillisecondOlderRevisionWrittenLastIsIgnored replayWithEmptyFlinkStateKeepsTheNewest
check "purge ignores record sequence" $main/CurrentStateRow.java \
  ".filter(FILTERS.value().range().endClosed(seq)))" ".filter(FILTERS.pass()))" \
  BigtableCurrentStateTest syncPurgesEarlierRecordsOnlyAndAnOlderPurgeCannotLowerIt accountDeletionPurgesEarlierRecordsAndHidesTheRest
check "account markers not applied" $main/BigtableCurrentStateSink.java \
  "if (CurrentStateRow.purges(event)) {" "if (false) {" \
  BigtableCurrentStateTest accountDeletionPurgesEarlierRecordsAndHidesTheRest
check "read ignores account status" $main/CurrentStateRow.java \
  '&& !"active".equals(EventJson.read(account.get(EVENT)).state)' "&& false" \
  CurrentStateApiTest anInactiveAccountHidesItsRecordsUntilReactivated
check "event_id includes arrival time" $main/JetstreamMapping.java \
  "event.entityType, event.entityId, event.revision, event.eventType)" \
  "event.entityType, event.entityId, event.revision, event.eventType, event.receivedAt)" \
  CurrentStateJobTest redeliveryKeepsTheEventIdAndDeleteNeedsNoRecord
check "rejected mutation ignored" $main/BigtableCurrentStateSink.java \
  "failure.compareAndSet(null, error);" "" \
  BigtableCurrentStateTest aRejectedMutationFailsTheFlush
check "commit despite a failed send" $main/TransactionalPublisher.java \
  "send.get();" "send.isDone();" \
  TransactionalPublisherTest anEarlierFailedSendBlocksTheCommitEvenIfLaterSendsSucceed
check "malformed message dropped" $main/TransactionalPublisher.java \
  "sends.add(producer.send(record));" "" \
  TransactionalPublisherTest malformedMessagesAreQuarantinedInTheSameTransaction
