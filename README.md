# EventProof GCP

Published as **The Sovereign Stream**:
<https://github.com/ReguiguiMohamed/The-Sovereign-Stream>. That is the public
repository name only; the technical identifiers in this project remain
`eventproof`, `dev.eventproof` and `operational-state.v1`.

EventProof is a correctness proof for operational event streams
targeted at Google Cloud. It demonstrates deterministic duplicate delivery and
out-of-order state updates before any company-specific adapter or cloud resource
is introduced.

The planned cloud route is Managed Service for Apache Kafka, Apache Flink on GKE,
Bigtable and a bounded Cloud Run query API. The region and external event contract
will be selected once the use case and data-residency
requirements are known.

There is no local Kafka broker in this project. Kafka is integrated only against
Google Cloud Managed Service for Apache Kafka, and only during the controlled GCP
staging and evidence window. The free local correctness path is the deterministic
generator, a Flink MiniCluster, the Bigtable emulator and a bounded local query
API.

## Current status

Implemented and tested locally:

- strict `operational-state.v1` JSON Schema;
- deterministic baseline, exact-duplicate and 120-second late scenarios;
- verification manifests recording arrival order, duplicate relationships and
  expected current state per entity;
- tests for schema validation, repeatability, identity, hashes, scenario semantics
  and invalid inputs;
- a Flink 2.2.1 job that reads `operational-state.v1` NDJSON, keys by `entity_id`
  and emits only events that advance current state;
- local MiniCluster tests for exact duplicate suppression and preservation of a
  newer state after an event arrives 120 seconds late.

Not implemented:

- any external adapter;
- Kafka, Bigtable, GKE or Cloud Run clients;
- containers, Terraform resources or deployment manifests;
- any cloud test or production-readiness claim.

No GCP resource has been created by this repository.

## Local commands

```bash
python -m pip install ".[test]"
python -m compileall -q eventproof tests
python -m unittest discover -s tests -v
mvn --batch-mode --file streaming/pom.xml verify
```

Generate deterministic baseline events:

```bash
python -m eventproof.simulator \
  --run-id local-smoke \
  --seed 20260824 \
  --count 10 \
  --output benchmarks/evidence/local-smoke.events.ndjson \
  --manifest benchmarks/evidence/local-smoke.manifest.ndjson
```

`--scenario duplicate` emits the first event again as the second arrival. The
copy has the same `event_id`, timestamps and canonical payload. A future consumer
must acknowledge it without applying it twice.

`--scenario late-120s` emits `completed` state for one entity, then delivers a
different `processing` event for that entity whose `event_time` is exactly 120
seconds older and whose `received_at` is later. Both unique events are accepted,
but expected current state remains `completed`.

Both failure scenarios require `--count` of at least 2.

Run generated NDJSON through the local Flink job with JDK 17 and Maven 3.8.6+
(Maven 3.9.16 was used for the recorded local run):

```bash
mvn --file streaming/pom.xml \
  -Devents.file=../benchmarks/evidence/local-smoke.events.ndjson \
  compile exec:exec
```

The path is resolved from the `streaming/` Maven module. The job prints only
current-state updates. A unique late event still counts as accepted in the
manifest, but it does not produce a current-state update when a newer event for
that entity already exists.

## Manifest

The manifest contains one `record: "event"` line per delivery followed by one
`record: "summary"` line. The summary records accepted-event count, duplicate and
late arrival indexes, and `expected_current_state` for every entity. These are
local expectations only; no streaming consumer enforces them yet.

## Repository boundaries

| Path | Responsibility |
| --- | --- |
| `eventproof/` | Event helpers and deterministic scenario generation |
| `contracts/` | Versioned schemas shared by future producers and consumers |
| `apps/` | Future external adapter and bounded query API |
| `streaming/` | Flink current-state job and local MiniCluster tests |
| `infra/` | Future Terraform roots after the local gate |
| `benchmarks/` | Untracked local evidence |
| `docs/` | Architecture and backlog |

## Safety

- Terraform `apply` and `destroy` require explicit human approval.
- Secrets, credentials, Terraform state and raw evidence are ignored.
- Cloud reports must distinguish designed, implemented locally, tested locally
  and executed in GCP.

The full delivery sequence and its cost gates are in
[`docs/end-to-end-plan.md`](docs/end-to-end-plan.md).
