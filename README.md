# grafana-zabbix-clickhouse-datasource

Open fork of the Grafana Zabbix plugin with an additional Direct DB path for ClickHouse.

## Goal

Keep the existing Zabbix datasource behavior for:

- host groups
- hosts
- items
- tags
- problems
- macros

and move only numeric history and trends reads to ClickHouse:

- `history`
- `history_uint`
- `trends`
- `trends_uint`

The metadata path stays on the regular Zabbix API. The heavy historical path goes through a Grafana ClickHouse datasource.

## Status

Early bootstrap.

Current repository already includes:

- upstream `grafana-zabbix` codebase as the base fork
- separate plugin ids so the fork can be installed next to the original plugin
- initial ClickHouse SQL dialect for direct history and trends queries
- config UI support for the official Grafana ClickHouse datasource type `grafana-clickhouse-datasource`
- self-managed numeric sync worker scaffold under `contrib/clickhouse-sync`

## MVP scope

1. Preserve current Zabbix API flows.
2. Add ClickHouse as a supported Direct DB datasource.
3. Support numeric tables only in the first iteration.
4. Validate the fork on a dedicated test Grafana and dedicated test ClickHouse instance.

More detail:

- [ClickHouse MVP design](docs/clickhouse-mvp.md)
- [Numeric replication design](docs/clickhouse-replication.md)

## Repository origin

This project is based on the open-source Grafana Zabbix plugin:

- upstream: [grafana/grafana-zabbix](https://github.com/grafana/grafana-zabbix)

The fork keeps Apache-2.0 licensing and will track upstream changes selectively.
