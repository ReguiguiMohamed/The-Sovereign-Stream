# ADR 0002: Bigtable current state

Status: accepted. Revised 17 September 2026: conditional writes replace cell
timestamps as the ordering mechanism, and account markers were added.
Sources: [schema design](https://docs.cloud.google.com/bigtable/docs/schema-design),
[writes](https://docs.cloud.google.com/bigtable/docs/writes),
[AT Protocol TIDs](https://atproto.com/specs/tid).

## Read pattern

The current state of one record, by `entity_type` and `entity_id`. One request
reads at most two rows: the record and its account. There is no scan on the read
path.

## Contract

| Element | Value |
| --- | --- |
| Row key | `escape(entity_type) + "#" + escape(entity_id)` |
| Column family | `cs`, garbage collection `maxVersions(1)` |
| `event` | the complete `record-state.v1` event as JSON |
| `revision` | the order key: a TID for records, the zero-padded sequence for account markers |
| `seq` | the zero-padded `source_seq`, compared against a purge boundary |
| `purge` | account rows only: the sequence up to which that account's records are removed |
| Cell timestamp | 0; one version per cell, replaced by a later accepted write |

Escaping replaces `%` with `%25`, then `#` with `%23`, so the one raw `#` is the
separator and distinct pairs never collide. Flink keys its state with the same
function.

## Why conditional writes

Cell timestamps cannot carry the order. A Bigtable cell timestamp is a whole
millisecond, while a TID holds microseconds and a clock id, so two different
revisions of one record can map to one version and the last write would win
regardless of revision.

Every write is therefore a check-and-mutate: the server compares the stored
`revision` with the new one and applies the mutation only if the new one is
greater. The comparison is on fixed-width, byte-ordered strings, so it is exact,
and the row is the only synchronisation point. Concurrent writers, retries, a
cold replay and expired Flink state all converge on the newest revision, and the
same revision written twice is a no-op.

Flink's keyed state sits in front of this to remove repeated writes. It is not
what makes the result correct.

## Account markers

An account event with `active: false, status: deleted`, and any sync event, raise
`purge` on the account row to that event's sequence, then delete every record row
of that account whose `seq` is at or below it, each delete conditional on that
comparison so later records survive. Reads hide a record whose `seq` is at or
below `purge`, and hide every record of an account whose stored status is not
`active`, so a replayed stale create cannot resurface.
