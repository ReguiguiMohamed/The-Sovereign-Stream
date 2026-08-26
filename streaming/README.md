# Streaming

This directory contains one Apache Flink 2.2.1 application built with JDK 17.

- `OperationalStateEvent` mirrors the versioned neutral event contract.
- `ParseOperationalState` maps one NDJSON line to that event type.
- `PreserveNewestState` keeps Flink managed state per `entity_id` and emits an
  update only when `event_time` is newer than the stored value.
- `CurrentStateJobTest` runs the complete operator in a local MiniCluster.

```bash
mvn --batch-mode --file streaming/pom.xml verify
```

The duplicate test sends the same event twice and expects one current-state
update. The late test sends `completed`, then a distinct `processing` event for
the same entity with an `event_time` exactly 120 seconds older, and expects the
single output to remain `completed`.

Kafka and Bigtable connectors are deliberately absent. Terraform and HTTP code
do not belong here.
