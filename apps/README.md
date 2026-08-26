# Applications

This directory is reserved for the selected external-event adapter. No
application exists until its external contract and local fixture test are added.

The bounded current-state query API lives in [`streaming/`](../streaming/README.md)
instead, because it reads the Bigtable row contract that the streaming job writes
and shares its event model. Flink processing does not belong here.
