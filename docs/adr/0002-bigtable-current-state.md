# ADR 0002: Bigtable current-state materialization

Status: accepted for local implementation against the Bigtable emulator (EP-014).
No GCP Bigtable instance exists. Sources accessed 26 August 2026:
[emulator](https://docs.cloud.google.com/bigtable/docs/emulator),
[schema design](https://docs.cloud.google.com/bigtable/docs/schema-design),
[Java client](https://docs.cloud.google.com/java/docs/reference/google-cloud-bigtable/latest/overview).

## Read pattern

The query this table exists to serve is a point read: *what is the current state
of one entity?* EP-015 will answer it with a single-row lookup by `entity_type`
and `entity_id`. There is no scan, no range query and no secondary access path,
so the schema is designed for one row read and nothing else.

## Contract

| Element | Value |
| --- | --- |
| Table | runtime configuration, `current-state` in tests |
| Row key | `escape(entity_type) + "#" + escape(entity_id)` |
| Column family | `cs` |
| Qualifier | `event` |
| Cell value | the complete `operational-state.v1` event as canonical JSON |
| Cell timestamp | the event's `event_time` in microseconds since the epoch |
| Garbage collection | `maxVersions(1)` |

### Row key

`entity_type` and `entity_id` are free-form non-empty strings, so plain
`type + "#" + id` is not injective: `("a#b", "c")` and `("a", "b#c")` both produce
`a#b#c`. Each component therefore has `%` escaped to `%25` and `#` to `%23`, in
that order, before joining. After escaping neither component contains a raw `#`,
so the single raw `#` is unambiguously the separator and distinct pairs always
produce distinct keys.

The key stays readable (`resource#resource-0000`). It is not hashed: hashing
costs readability and is the remedy for a measured hotspot, which this project
has not measured.

### Cell value

The whole event is stored as one JSON cell rather than spread across a column per
field, because the bounded query reads the whole event together. It is written
with the same `EventJson` mapper the parser uses, so stored JSON keeps the
snake_case contract field names and reads back into the same type.

### Cell timestamp and write semantics

The cell timestamp is `event_time`, never arrival time. Arrival time would make
the newest write win, which is the failure this project exists to prevent.

With `maxVersions(1)` and event-time cell timestamps, a mutation carrying an
older `event_time` writes an older cell version and cannot become the newest
cell, no matter when it arrives. Garbage collection is asynchronous, so an older
cell may still be present after a newer one is written; reads therefore select
the newest cell explicitly rather than trusting collection to have run.

Writes are idempotent: row key, qualifier and cell timestamp are all derived from
the event, so writing the same event twice produces the same single cell. This is
idempotent materialization and newest-event-time visibility. It is **not**
exactly-once delivery, and nothing here claims that.

## Truth boundary

Tested locally against the official bundled emulator. The emulator is in-memory:
Google documents that its data does not persist across runs and that it is not
for production use. This ADR therefore proves the data contract, the client
behaviour and late-write materialization semantics. It proves nothing about
durability across a restart, which stays with EP-016 and the controlled GCP
evidence window.

## Known limits

- Equal `event_time` for the same entity: `PreserveNewestState` emits only on a
  strictly newer `event_time`, so through the Flink path the first arrival wins
  and the second never reaches Bigtable. A direct write of a second event at the
  identical cell timestamp would overwrite the first. This is a recorded contract
  limit, not a designed total ordering; defining one is deferred until a real
  workload shows equal timestamps occur.
- Throughput, row-key distribution and hotspot behaviour are unmeasured. A
  workload-specific key distribution decision waits for production-shaped data.
- One synchronous mutation per record. Batching is deferred to a measurement.
