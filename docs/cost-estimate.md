# Cost estimate

Worked out on 17 September 2026, before the window opened, for the stack in
`europe-west1`. Prices are USD list prices before trial credit, from the Cloud
Billing Catalog API (*catalog*) or Google's pricing pages (*page*). Traffic comes
from the [Jetstream sample](#jetstream-sample). 1 GB = 0.931 GiB and a month is
730 hours.

## Traffic

| Flow | Median | p95 | Basis |
| --- | ---: | ---: | --- |
| Jetstream into the producer, through Cloud NAT | 0.83 GiB/h | 3.94 GiB/h | measured bytes |
| Produced to Kafka | 0.70 GiB/h | 2.66 GiB/h | 375 or 1,416 records/s × 560 bytes, uncompressed |

## Per hour

| Item | Basis | USD/h |
| --- | --- | ---: |
| Kafka compute, 3 vCPU + 3 GiB | 2.1 DCU × 0.099 *catalog* | 0.208 |
| Kafka local storage | 100 GB per vCPU whatever the data size *page*, so 279.4 GiB × 0.17 per GiB-month *catalog* | 0.065 |
| Kafka long-term storage | one replica of produced data, ≤ 192 GiB after 72 h at p95, × 0.10 per GiB-month *catalog* | ≤ 0.026 |
| Kafka replication | produced × 2 replicas × 0.01 per GiB *page* | 0.014 to 0.053 |
| Private Service Connect data processing | produced + consumed × ≤ 0.01 per GiB, endpoint hours waived *page* | 0.014 to 0.053 |
| GKE zonal cluster fee | 0.10 *catalog* | 0.100 |
| GKE node, e2-standard-4 | 4 × 0.02399 + 16 × 0.003216 *catalog* | 0.147 |
| Node disk, 50 GiB pd-balanced | 0.000137 per GiB-hour *page*, us-central1 figure | 0.007 |
| Cloud NAT gateway, one VM | 0.0014 *page* | 0.001 |
| Cloud NAT external IP | 0.005 *catalog* | 0.005 |
| Cloud NAT data processing | Jetstream flow × 0.045 per GiB *page* | 0.037 to 0.177 |
| Bigtable free-trial instance | trial node and storage SKUs at 0.00 *catalog* | 0.000 |
| Checkpoint bucket | ≤ 10 GiB of RocksDB state, written every 30 s | ≤ 0.003 |
| Artifact Registry | ≤ 10 GiB of images × 0.10 per GiB-month | ≤ 0.002 |
| Cloud Build | teardown gate, ≤ 2 build-minutes an hour × 0.006 | ≤ 0.012 |
| Logging, metrics, Cloud Run | inside the free allotments at this volume | ~0 |
| **Total** | | **0.64 to 0.86** |

Kafka is USD 0.33 to 0.41 of every hour, and its compute and local storage are
fixed by the 3-vCPU minimum. The Bigtable node cost nothing because it was the
project's free-trial instance.

## Per window, at p95

| Window | USD |
| --- | ---: |
| 2-hour smoke | 1.7 |
| 24 hours | 21 |
| 48 hours | 41 |
| 72 hours | 62 |

Every project on the billing account spent the same trial credit, so the budget
alerts in [bootstrap.sh](../infra/bootstrap.sh) tracked gross cost for the whole
account as well as for this project.

## Jetstream sample

Build `f7158e7e-348b-4602-980c-0d7e9a2521b9` ran
[jetstream-sample.yaml](../tools/jetstream-sample.yaml) for 900 seconds from
Cloud Build in `europe-west1`, filtered to posts, likes and reposts.

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
| Resume | cursor 25984748796, and the first event after reconnecting carried the same seq, so 1 redelivery |
| Records deleted after being created in the window | 2,544 |

At the median rate that is 32.4 million messages and 20 GiB of raw JSON a day.
