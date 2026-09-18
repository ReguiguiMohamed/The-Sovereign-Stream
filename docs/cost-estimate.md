# Cost estimate — evidence stack, europe-west1

Status: estimate. Actual billing is reconciled after the window. Prices are USD
list prices, gross of trial credit, from the Cloud Billing Catalog API (*catalog*)
or Google's pricing pages (*page*) on 17 September 2026. Traffic comes from the
[Jetstream sample](#jetstream-sample). 1 GB = 0.931 GiB; a month is 730 hours.

## Traffic inputs

| Flow | Median | p95 | Basis |
| --- | ---: | ---: | --- |
| Jetstream into the producer, through Cloud NAT | 0.83 GiB/h | 3.94 GiB/h | measured bytes |
| Produced to Kafka | 0.70 GiB/h | 2.66 GiB/h | 375 / 1,416 records/s × 560 bytes, uncompressed upper bound |

## Hourly

| Item | Basis | USD/h |
| --- | --- | ---: |
| Kafka compute, 3 vCPU + 3 GiB | 2.1 DCU × 0.099 *catalog* | 0.208 |
| Kafka local storage | 100 GB per vCPU, billed whatever the data size *page*: 279.4 GiB × 0.17 per GiB-month *catalog* | 0.065 |
| Kafka long-term storage | one replica of produced data, ≤ 192 GiB after 72 h at p95 × 0.10 per GiB-month *catalog* | ≤ 0.026 |
| Kafka replication | produced × 2 replicas × 0.01 per GiB *page* | 0.014–0.053 |
| Private Service Connect data processing | produced + consumed × ≤ 0.01 per GiB; endpoint hours waived *page* | 0.014–0.053 |
| GKE zonal cluster fee | 0.10 *catalog* | 0.100 |
| GKE node, e2-standard-4 | 4 × 0.02399 + 16 × 0.003216 *catalog* | 0.147 |
| Node disk, 50 GiB pd-balanced | 0.000137 per GiB-hour *page*, us-central1 figure | 0.007 |
| Cloud NAT gateway, one VM | 0.0014 *page* | 0.001 |
| Cloud NAT external IP | 0.005 *catalog* | 0.005 |
| Cloud NAT data processing | Jetstream flow × 0.045 per GiB, both directions *page* | 0.037–0.177 |
| Bigtable free trial instance | trial node and storage SKUs at 0.00 *catalog* | 0.000 |
| Checkpoint bucket | ≤ 10 GiB of RocksDB state plus writes every 30 s | ≤ 0.003 |
| Artifact Registry | ≤ 10 GiB of images × 0.10 per GiB-month | ≤ 0.002 |
| Cloud Build | teardown gate, ≤ 2 build-minutes per hour × 0.006 | ≤ 0.012 |
| Logging, metrics, Cloud Run | inside the free allotments at this volume | ~0 |
| **Total** | | **0.64–0.86** |

Kafka is USD 0.33–0.41 of every hour; its compute and local storage are fixed by
the 3-vCPU minimum. No paid Bigtable node is planned; without the trial
instance the stack does not start.

## Windows, at p95

| Window | USD |
| --- | ---: |
| 2-hour smoke | 1.7 |
| 24 hours | 21 |
| 48 hours | 41 |
| 72 hours | 62 |

The USD 80 evidence allocation covers 72 hours at p95, if the trial credit
remaining on the billing account covers it too. Every project on that account
spends the same credit, so the account-wide budget tracks gross cost for all of
them.

## Controls

- Budget alerts on gross cost, credits excluded: one for the whole billing
  account, one for this project ([bootstrap](../infra/bootstrap.sh)). They alert;
  they do not cap spending.
- The scheduled teardown destroys the evidence stack at the window end and
  retries until its inventory is empty ([control root](../infra/control/main.tf)).

## Spent so far

Cloud Build on the default pool: 33.3 build-minutes by the first checkpoint,
plus the correction builds listed in the checkpoint. At list price, USD 0.006 a
minute, before any free-tier minutes left on the account. Storage: build
sources, logs, images and the Jetstream capture, under 10 GiB.

## Jetstream sample

Build `f7158e7e-348b-4602-980c-0d7e9a2521b9`, 900 seconds from Cloud Build in
europe-west1, filtered to posts, likes and reposts, collected by a Python client.

| Measure | Value |
| --- | --- |
| Messages per second, min / median / p95 per minute | 348 / 375 / 1,416 |
| Bytes per second, min / median / p95 | 227,880 / 246,309 / 1,175,913 |
| Bytes per message | 691 |
| Unique commits | 411,821 |
| Mix | likes 252,805 created, 7,156 deleted; posts 107,812 created, 1,722 deleted, 17 updated; reposts 40,763 created, 1,546 deleted |
| Other message kinds | account 293, identity 249, sync 2 |
| Source lag, median / p95 / max | 0.04 s / 4.5 s / 10.4 s |
| Parse failures | 0 |
| Revision regressions per record | 0 |
| Resume | cursor 25984748796; the first event after reconnecting had the same seq; 1 redelivery |
| Records deleted after being created in the window | 2,544 |

At the median rate, that is 32.4 million messages and 20 GiB of raw JSON a day.
The capture and summary are in
`gs://eventproof-stream-2609-capture/jetstream-sample/f7158e7e-348b-4602-980c-0d7e9a2521b9/`.
