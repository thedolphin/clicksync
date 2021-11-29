# clicksync
Tool for synchronizing clickhouse clusters

- works only with partitioned MergeTree tables
- uses info from ```system.clusters``` to determine nodes
- creates tables on destination cluster
- recreates tables if schema differ
- checks record number in each partition each run, and refill if numbers differ (sync)
- can work in two modes: (a) suck data in and put it to destination cluster (very slow) and (b) direct, using insert ... select from remote()
- can copy only recently created partitions

# usage

adopt ```clicksync.yaml``` as you need, then run

```
clicsync.py clicksync.yaml
```
