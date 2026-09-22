# Tools

| File | What it does |
| --- | --- |
| [`image-smoke.sh`](image-smoke.sh) | Runs the packaged images end to end inside Cloud Build before any image is pushed |
| [`discrimination-check.sh`](discrimination-check.sh) | Puts known bugs back into the code and requires the tests to catch each one |
| [`jetstream-sample.yaml`](jetstream-sample.yaml) | A Cloud Build job that measured 15 minutes of the live feed |

## Smoke test

The `smoke` step of [`cloudbuild.yaml`](../cloudbuild.yaml) wires the two
images together on the build's own Docker network: the producer against live
Jetstream, a Kafka broker, the Flink job image, the Bigtable emulator and the
API. After two and a half minutes it requires all of this:

- the producer published events
- the API serves posts the Flink job wrote
- the topic holds at least as many records as the producer says it committed
- Flink completed a checkpoint

Then it restarts the producer, which has to resume from the highest committed
sequence and move past it.

It also compares identities. A source redelivery repeats an `event_id` on
purpose, so repeats are reported, and the step fails only if one id covers two
different source events.

Build `5b76afc3-d263-45bb-99f2-e06ea438c9e1`, for example, passed 34 tests, found
68,527 records carrying 68,527 distinct ids for 68,527 committed, and had the
API serve 20 of 20 sampled posts.

## Mutation check

`discrimination-check.sh` makes 11 small edits to the source, one at a time,
each reintroducing a bug the tests are meant to catch: state keyed by id without
its type, arrival order beating revision order, an unconditional Bigtable write,
a purge that ignores sequence, a commit after a failed send, and six more. Each
edit must make its named tests fail with a JUnit assertion. A compile error or a
crash fails the check instead of passing it.

It runs by hand from the CI workflow: Actions, CI, Run workflow. On
22 September 2026 it caught all 11 ([run 35784341235](https://github.com/ReguiguiMohamed/The-Sovereign-Stream/actions/runs/35784341235)).

## Jetstream sample

`jetstream-sample.yaml` read posts, likes and reposts from Cloud Build in
`europe-west1` for 900 seconds with one forced reconnect, and stored a capture
without record bodies. The numbers are in the
[cost estimate](../docs/cost-estimate.md#jetstream-sample).
