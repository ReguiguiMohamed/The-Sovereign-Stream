# The Sovereign Stream

Operational event streams redeliver and reorder. This repository proves a stream
processor handles both correctly, and shows the evidence, on a production-shaped
Google Cloud architecture.

Code namespace: `eventproof`.

## The problem

Any at-least-once pipeline eventually delivers the same event twice, and any
distributed producer eventually delivers an older event after a newer one. Most
streaming demonstrations sidestep both by measuring throughput on clean, ordered
input. Correctness under redelivery and reordering is what actually breaks
systems in production, so that is what this project measures.

## The two invariants

The project exists to hold these under automated test:

1. **An exact redelivery is acknowledged but never applied twice.** A duplicate
   event produces no second current-state update.
2. **A late event never overwrites newer state.** An event whose `event_time` is
   120 seconds older than the entity's current state is accepted as delivered,
   but the newer state stands.

Both are evaluated per entity, from deterministic input, with the expected result
recorded in a manifest before the stream processor runs.

## Approach

Correctness first, infrastructure second. State transitions are proved in a local
Flink MiniCluster against a deterministic generator before any broker, container
or cloud resource is introduced. Ingestion plumbing cannot conceal a wrong state
transition when the transition is already under test.

Every claim carries an explicit boundary: designed only, implemented locally,
tested locally, or executed in GCP. Nothing is described as working in the cloud
until it has run there and produced timestamped evidence.

## Architecture

Target path:

```text
event source
  -> Managed Service for Apache Kafka
  -> Apache Flink 2.2.1 on GKE
  -> Bigtable current state
  -> bounded Cloud Run query API
```

GCS holds checkpoints and savepoints, Managed Prometheus holds runtime metrics,
and Terraform defines every resource.

Local path, which is free, needs no broker, and gates every cloud step:

```text
deterministic generator (NDJSON)
  -> Apache Flink 2.2.1 MiniCluster
  -> Bigtable emulator current state
  -> bounded local query API
```

Kafka is integrated in exactly one place: Google Cloud Managed Service for Apache
Kafka, during a controlled, time-capped evidence window. There is no local broker.

## Status

| Capability | Boundary |
| --- | --- |
| `operational-state.v1` contract and canonical encoding | Tested locally |
| Deterministic baseline, duplicate and late-120s scenarios | Tested locally |
| Flink current-state job with keyed newest-wins state | Tested locally |
| Duplicate suppression and per-entity late-state preservation | Tested locally |
| Bigtable materialization and bounded query API | Designed only |
| Checkpoint and restart recovery, metrics, containers | Designed only |
| Managed Kafka, GKE, Cloud Run and Terraform resources | Designed only |
| Every component above, in Google Cloud | Not executed in GCP |

No GCP resource has been created by this repository. No exactly-once processing,
production readiness or cloud validation is claimed.

## Quick start

Requires Python 3.12+, JDK 17 and Maven 3.8.6+.

```bash
python -m pip install ".[test]"
python -m unittest discover -s tests
mvn --batch-mode --file streaming/pom.xml verify
```

Run a late delivery end to end:

```bash
python -m eventproof.simulator \
  --run-id demo --seed 20260826 --count 3 --scenario late-120s \
  --output benchmarks/evidence/demo.events.ndjson \
  --manifest benchmarks/evidence/demo.manifest.ndjson

mvn --file streaming/pom.xml \
  -Devents.file=../benchmarks/evidence/demo.events.ndjson \
  compile exec:exec
```

The job prints current-state updates only. The event delivered 120 seconds late
produces none, and the printed result matches `expected_current_state` in the
generated manifest.

## Repository map

| Path | Contents |
| --- | --- |
| [`contracts/`](contracts/) | Versioned event schemas |
| [`eventproof/`](eventproof/README.md) | Event model and deterministic scenario generator |
| [`streaming/`](streaming/README.md) | Flink current-state job and MiniCluster tests |
| [`apps/`](apps/README.md) | External adapter and bounded query API |
| [`infra/`](infra/README.md) | Terraform roots |
| [`docs/`](docs/) | Architecture decision, delivery plan and backlog |

The delivery sequence, cost gates and truth boundaries are in
[`docs/end-to-end-plan.md`](docs/end-to-end-plan.md).

## License

MIT. See [LICENSE](LICENSE).
