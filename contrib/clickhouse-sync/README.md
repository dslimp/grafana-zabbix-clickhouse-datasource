# ClickHouse sync worker

This directory contains a self-managed numeric replication worker for the
`grafana-zabbix-clickhouse-datasource` project.

The worker is intentionally separate from the Grafana plugin runtime.

It is designed for:

- initial backfill of Zabbix numeric tables into ClickHouse using `itemid` slices
- recurring catch-up with a bounded overlap window
- periodic repair of a recent time window
- dedup-aware writes into `ReplacingMergeTree(_version)` tables

Supported source families:

- MySQL or MariaDB
- PostgreSQL

Supported target tables:

- `history`
- `history_uint`
- `trends`
- `trends_uint`

## State model

The worker stores state in a local JSON file.

Per table it tracks:

- `last_clock`
- last composite cursor
- `backfill_completed`
- timestamps of the last successful run

This makes the worker restart-safe and allows recurring catch-up without relying
on external CDC products.

## Modes

- `backfill`: import from the configured start watermark forward
- `catchup`: continue from the last watermark with overlap
- `repair`: reread a recent window and refresh rows with a newer `_version`
- `daemon`: run `catchup` on a schedule and `repair` every N iterations

## Config

See [config.example.yaml](/tmp/grafana-zabbix-clickhouse-datasource/contrib/clickhouse-sync/config.example.yaml).

## Current scope

This worker is numeric-only on purpose.

Metadata remains in Zabbix API. String and log history stay out of the first
ClickHouse phase.
