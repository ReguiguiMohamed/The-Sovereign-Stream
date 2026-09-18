# Streaming

One Maven module on Flink 2.2.1 and JDK 17. It builds a single shaded jar with
three entrypoints.

| Entrypoint | Runs on | Does |
| --- | --- | --- |
| `JetstreamProducer` | GKE | Reads Bluesky Jetstream v2 and publishes `record-state.v1` in Kafka transactions |
| `CurrentStateJob` | Flink on GKE | Kafka topic → newest revision per record → Bigtable |
| `CurrentStateApi` | Cloud Run | `GET /v1/current-state?entity_type=&entity_id=` |

Only `CurrentStateJob` and `ParseRecordState` use Flink; the producer and the API
run on a plain JRE.

## Record contract

[`contracts/record-state.v1.schema.json`](../contracts/record-state.v1.schema.json).
One event is one revision of one record, or one account-level marker.

| Field | Record | Account marker |
| --- | --- | --- |
| `entity_type` | `collection`: post, like or repost | `account` or `sync` |
| `entity_id` | `did/rkey` | `did` |
| `event_type` | `create`, `update`, `delete` | `account` or `sync` |
| `state` | `active`, or `deleted` for a delete | `active`, the source status, or `resynced` |
| `revision` | `rev`, the repository commit TID | the zero-padded `seq` |
| `source_seq` | Jetstream `seq`, the resume cursor | same |
| `subject` | the liked or reposted post URI, otherwise null | null |
| `event_id` | SHA-256 of entity type, id, revision and event type | same |

Record bodies are not kept. Identity events change no record and are counted,
not published. Anything else malformed goes to the quarantine topic.

## Ordering

`escape(entity_type)#escape(entity_id)` keys Flink state and the Bigtable row.

Flink emits a revision only when it sorts after the last one emitted for that
record, so a redelivery is dropped and arrival order does not matter. Its state
expires after 3 days, the topic retention.

Storage does not depend on that. Every write is a check-and-mutate: Bigtable
compares the stored revision with the new one and applies the mutation only when
the new one is greater. Two revisions inside one millisecond, a batch retry, a
cold replay and expired Flink state all converge on the newest revision.

## Account lifecycle

An account event with `active: false, status: deleted`, or a sync event, removes
the account's earlier records: the account row's purge boundary is raised to that
sequence, and each of the account's record rows whose sequence is at or below it
is deleted, conditionally, so later records stay. A record read also checks the
account row, so a record is hidden while the account is inactive and after a
purge, even if a replay rewrites it.

## Producer delivery

- One Kafka transaction per batch. The batch commits only after every send in it
  has succeeded, so a failed send cannot be passed by a later one.
- The cursor is the highest committed sequence; on start it is read back from the
  record topic with `read_committed`.
- The Jetstream cursor is inclusive, so the boundary event is redelivered and
  dropped by its revision.
- The transactional id fences a previous instance, and any Kafka failure ends the
  process; the restart resumes from the committed cursor.
- One JSON stats line a minute: received, published, identity-skipped,
  quarantined, cursor.

## Query API

| Case | Response |
| --- | --- |
| record present and visible | `200`, the stored event |
| absent, purged, or account inactive | `404 {"error":"not_found"}` |
| missing, blank or repeated parameter | `400 {"error":"invalid_request"}` |
| method other than GET | `405` with `Allow: GET` |

One request reads at most two rows: the record and its account. The server uses
16 workers, matching Cloud Run concurrency, and shuts down on SIGTERM. Access
control is Cloud Run IAM.

## Tests

[`cloudbuild.yaml`](../cloudbuild.yaml) runs `mvn verify`, builds both images and
then [`tools/image-smoke.sh`](../tools/image-smoke.sh), which runs the packaged
images against live Jetstream, a Kafka broker and the Bigtable emulator, and
restarts the producer to check its resume cursor.

- `CurrentStateJobTest`: redelivery, out-of-order revisions, delete convergence,
  type isolation, Jetstream mapping, account and sync markers, rejected input.
- `BigtableCurrentStateTest`: same-millisecond and concurrent writes, stale
  create after delete, replay with empty Flink state, account purge, flush
  failure.
- `CurrentStateApiTest`: the HTTP contract and account visibility.
- `TransactionalPublisherTest`: failed send, failed commit, quarantine, resume.
- `SavepointRecoveryTest`: keyed state restored from a savepoint into a job with
  another source suppresses an older revision.

[`tools/discrimination-check.sh`](../tools/discrimination-check.sh) reintroduces
each guarded defect and requires the named tests to report an assertion failure.
