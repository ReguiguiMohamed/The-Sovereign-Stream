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
- `CurrentStateApi` serves the bounded current-state lookup.
- `CurrentStateApiTest` drives it over HTTP against the emulator.

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

## Query API

`CurrentStateApi` exposes exactly one endpoint on the JDK HTTP server:

```text
GET /v1/current-state?entity_type=<value>&entity_id=<value>
```

| Case | Response |
| --- | --- |
| entity present | `200`, the complete `operational-state.v1` event |
| entity absent | `404 {"error":"not_found"}` |
| missing, blank or repeated parameter | `400 {"error":"invalid_request"}` |
| any method other than GET | `405 {"error":"method_not_allowed"}` with `Allow: GET` |

Successful responses are `application/json; charset=utf-8`. Every request is one
point read of one row: there is no scan, listing, pagination, mutation, health or
admin endpoint, so query cost does not grow with the table. Failures return a
fixed error body and never an exception message.

It binds to loopback and has no authentication, which is why it is a local proof
and not a deployable service. Configuration comes from `BIGTABLE_PROJECT_ID`,
`BIGTABLE_INSTANCE_ID`, `BIGTABLE_TABLE_ID`, `PORT` and, for local runs,
`BIGTABLE_EMULATOR_HOST`.

The Bigtable row contract is recorded in
[`docs/adr/0002-bigtable-current-state.md`](../docs/adr/0002-bigtable-current-state.md).
The emulator is in-memory, so these tests prove the data contract, the
materialization semantics and the HTTP contract, not durability across a restart.
No latency, throughput or cost has been measured.

Kafka connectors are deliberately absent. Terraform and HTTP code do not belong
here.
