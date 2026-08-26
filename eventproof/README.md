# eventproof

The event model and the deterministic scenario generator. This package produces
the fixed input and the expected result that the streaming tests are checked
against.

- `event.py` — immutable `OperationalState` events, canonical JSON encoding and
  SHA-256 payload hashes. Canonical encoding is sorted, separator-tight and
  UTF-8, so an identical event always produces an identical `event_id`.
- `simulator.py` — deterministic NDJSON scenario generation and the verification
  manifest.

## Generating events

```bash
python -m eventproof.simulator \
  --run-id local-smoke \
  --seed 20260824 \
  --count 10 \
  --output benchmarks/evidence/local-smoke.events.ndjson \
  --manifest benchmarks/evidence/local-smoke.manifest.ndjson
```

The same `--run-id`, `--seed`, `--count`, `--start-time` and `--scenario` always
produce byte-identical output.

## Scenarios

| `--scenario` | Behaviour |
| --- | --- |
| `baseline` | One event per entity at a fixed interval. |
| `duplicate` | The first event is emitted again as the second arrival, with the same `event_id`, timestamps and canonical payload. A consumer must acknowledge the redelivery without applying it twice. |
| `late-120s` | A `completed` event is emitted for one entity, then a distinct `processing` event for that same entity whose `event_time` is exactly 120 seconds older and whose `received_at` is later. Both are accepted as delivered, but expected current state remains `completed`. |

`duplicate` and `late-120s` require `--count` of at least 2.

## Manifest format

The manifest is NDJSON: one `record: "event"` line per delivery, then one
`record: "summary"` line.

Each `event` record carries the arrival index, the payload hash, the arrival
index this delivery duplicates (or `null`), and how many seconds late it was
relative to existing state for its entity.

The `summary` record carries the scenario, the emitted and expected-accepted
counts, the duplicate and late arrival indexes, and `expected_current_state` for
every entity.

The manifest is the expectation, written before the stream processor runs. It is
what the Flink output is compared against; nothing in this package enforces it.

## Contract

Events conform to [`contracts/operational-state.v1.schema.json`](../contracts/operational-state.v1.schema.json),
which is strict: `additionalProperties` is false and every field is required.
The Java counterpart is `dev.eventproof.streaming.OperationalStateEvent` in
[`streaming/`](../streaming/README.md).
