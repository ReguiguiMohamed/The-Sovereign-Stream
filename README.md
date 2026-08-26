# The Sovereign Stream

Operational event streams redeliver and reorder. This repository proves a stream
processor handles both correctly, on a production-shaped Google Cloud
architecture.

Code namespace: `eventproof`.

## The problem

Any at-least-once pipeline eventually delivers the same event twice, and any
distributed producer eventually delivers an older event after a newer one. Most
streaming demonstrations measure throughput on clean, ordered input. This one
measures correctness under redelivery and reordering.

## The two invariants

1. **An exact redelivery is acknowledged but never applied twice.** A duplicate
   event produces no second current-state update.
2. **A late event never overwrites newer state.** An event whose `event_time` is
   120 seconds older than the entity's current state is accepted as delivered,
   and the newer state stands.

Both are evaluated per entity, from deterministic input, with the expected result
recorded in a manifest before the stream processor runs.

## Approach

Correctness first, infrastructure second. State transitions are proved in a local
Flink MiniCluster against deterministic input before a broker, container or cloud
resource enters the picture: a wrong transition is easiest to catch when nothing
else can hide it.

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
and Terraform defines every resource. Kafka ingestion is exercised against
Managed Service for Apache Kafka inside a time-capped evidence window.

Local path, which gates every cloud step:

```text
deterministic generator (NDJSON)
  -> Apache Flink 2.2.1 MiniCluster
  -> Bigtable emulator current state
  -> bounded local query API
```

## Done

- `operational-state.v1` contract with canonical JSON encoding and SHA-256 event
  identity.
- Deterministic generator producing baseline, exact-duplicate and 120-second-late
  scenarios, plus a verification manifest recording expected current state per
  entity.
- Apache Flink 2.2.1 job on JDK 17: NDJSON parsing, keyed per-entity state and
  newest-wins current-state emission.
- MiniCluster tests proving both invariants, and a generator-to-Flink run whose
  output matches the manifest.
- CI reproducing the Python and Flink gates on every push.

## Next

- Materialize current state into the Bigtable emulator, then serve one entity
  through a bounded query API.
- Checkpointing with restart and recovery tests, plus accepted, suppressed and
  late counters.
- Container images for the streaming job and the query API.
- Terraform for identity, network, retained storage and time-capped compute.
- Managed Kafka ingestion and replay, exercised in a GCP evidence window.

Sequencing and cost gates are in [`docs/end-to-end-plan.md`](docs/end-to-end-plan.md);
the item-level breakdown is in [`docs/backlog.md`](docs/backlog.md).

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

## License

MIT. See [LICENSE](LICENSE).
