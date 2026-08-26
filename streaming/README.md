# Streaming

This directory contains one Apache Flink 2.2.1 application built with JDK 17.

- `OperationalStateEvent` mirrors the versioned neutral event contract.
- `ParseOperationalState` maps one NDJSON line to that event type.
- `PreserveNewestState` keeps Flink managed state per `entity_id` and emits an
  update only when `event_time` is newer than the stored value.
- `CurrentStateRow` is the Bigtable row contract: row key, column family, the
  event JSON cell and its `event_time` cell timestamp.
- `BigtableCurrentStateSink` writes accepted updates to Bigtable, one client per
  sink writer.
- `CurrentStateJobTest` runs the complete operator in a local MiniCluster.
- `BigtableCurrentStateTest` runs the same operator into the official Bigtable
  emulator, which the test starts in-process on a free port.

```bash
mvn --batch-mode --file streaming/pom.xml verify
```

That single command runs everything, including the emulator tests: the emulator
binary ships inside `google-cloud-bigtable-emulator`, so no Docker, no `gcloud`
and no credentials are needed.

The duplicate test sends the same event twice and expects one current-state
update. The late test sends `completed`, then a distinct `processing` event for
the same entity with an `event_time` exactly 120 seconds older, and expects the
single output to remain `completed`.

The Bigtable row contract is recorded in
[`docs/adr/0002-bigtable-current-state.md`](../docs/adr/0002-bigtable-current-state.md).
The emulator is in-memory, so these tests prove the data contract and
materialization semantics, not durability across a restart.

Kafka connectors are deliberately absent. Terraform and HTTP code do not belong
here.
