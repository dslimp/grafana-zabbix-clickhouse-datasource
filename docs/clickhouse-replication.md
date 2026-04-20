# Numeric replication design

## Test target

Initial test target:

- `zbx.infra.unitedline.net`

This host already serves as an isolated compare environment for Zabbix data migration tests and is suitable for a dedicated ClickHouse instance.

## Replication scope

Replicate only numeric data:

- `history`
- `history_uint`
- `trends`
- `trends_uint`

Metadata continues to come from Zabbix API. String and log history are intentionally excluded from the first phase.

## Sources

Two source paths must be supported:

### 1. MySQL or MariaDB source

Primary use:

- direct import from a Zabbix MySQL or MariaDB source
- or from a local MySQL clone produced via `mariadb-backup`

### 2. PostgreSQL source

Primary use:

- import from an existing PostgreSQL or PostgreSQL plus Timescale compare database

This keeps the ClickHouse pipeline usable both for existing MySQL-backed Zabbix and for already-migrated PostgreSQL-backed environments.

## Why not rely on native MySQL replication inside ClickHouse

There used to be a native experimental path around `MaterializedMySQL`.

It is not a good foundation for this project because:

- it is MySQL-specific and does not solve PostgreSQL to ClickHouse replication
- it was experimental
- recent ClickHouse builds and downstream distributions have removed or deprecated it

So this project should rely on an external replication worker instead of a ClickHouse-native MySQL database engine.

## Replication model

Use a two-phase pipeline.

### Initial backfill

- read tables in batches by `itemid` slices
- optionally narrow by `clock` windows
- load into ClickHouse in bulk
- store checkpoints outside the plugin

### Catch-up

- continue from the last imported `clock`
- re-read a small overlap window to absorb delayed rows
- write rows with a higher replication version
- let the query layer deduplicate by natural key

## Recommended ClickHouse storage model

Use one table per Zabbix numeric source table.

Recommended key shape:

- `ORDER BY (itemid, clock)`

Recommended partitioning:

- monthly partition on `toYYYYMM(toDateTime(clock))`

Recommended engine model:

- `ReplacingMergeTree(_version)`

Recommended natural keys:

- `history` and `history_uint`: `(itemid, clock, ns)`
- `trends` and `trends_uint`: `(itemid, clock)`

This matches the main query pattern used by the direct DB connector:

- filter by `itemid`
- filter by time range
- aggregate by rounded time bucket

It also keeps the replication path practical:

- forward catch-up can reuse a bounded overlap window
- current-hour trend rows can be refreshed
- duplicates can be collapsed in the query layer with `argMax(..., _version)`

## First implementation choice

Start with an external replication worker, not an in-plugin replication engine.

Reasons:

- keeps Grafana plugin focused on reads
- makes replication reusable outside Grafana
- allows stronger backfill and catch-up controls
- avoids coupling plugin release cadence to data-loading logic

## Planned service shape

The first self-managed implementation should run as a small host-side service.

It does not need Kafka, Debezium, ClickPipes, PeerDB, or another external
replication product.

Core shape:

- one config file
- one state file
- one process
- recurring `catchup`
- periodic `repair`

The first scaffold for this service lives in:

- `contrib/clickhouse-sync/clickhouse_sync.py`

## Continuous replication after initial load

Do not stop at a one-shot importer.

The steady-state design should use two recurring loops:

### Near-real-time catch-up loop

- run every few minutes
- read forward from the last watermark
- include a bounded overlap window
- insert rows with a fresh `_version`

### Periodic repair loop

- re-read a wider recent window, for example the last few hours
- refresh rows that may arrive late from proxies or delayed ingestion paths
- refresh current-hour trend rows that can still change

## First test sequence

1. Install ClickHouse on `zbx.infra.unitedline.net`.
2. Create numeric Zabbix-compatible tables.
3. Run initial backfill from the current compare data source.
4. Validate the forked datasource against the test ClickHouse datasource.
5. Measure side-by-side performance against the PostgreSQL compare path.
