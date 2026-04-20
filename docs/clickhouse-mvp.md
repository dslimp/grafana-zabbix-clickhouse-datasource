# ClickHouse MVP

## Objective

Build a fork of the Zabbix Grafana plugin that keeps Zabbix API as the source of truth for metadata and uses ClickHouse only for direct historical reads.

## Query split

Use Zabbix API for:

- groups
- hosts
- applications
- items
- macros
- problems
- alerts

Use ClickHouse for:

- `history`
- `history_uint`
- `trends`
- `trends_uint`

Do not use ClickHouse in the first MVP for:

- `history_str`
- `history_text`
- `history_log`

## Grafana datasource assumptions

The fork expects an existing Grafana datasource of type:

- `grafana-clickhouse-datasource`

The Zabbix datasource keeps the same UI and query editor model. Only the Direct DB connector layer changes.

## Required ClickHouse table contract

The first MVP assumes plain numeric Zabbix-like tables with these columns:

- `itemid`
- `clock`
- `value` for `history` and `history_uint`
- `num`, `value_min`, `value_avg`, `value_max` for `trends` and `trends_uint`

The SQL dialect groups points by a rounded bucket derived from `clock`.

## Implementation stages

1. Add ClickHouse datasource type to Direct DB datasource selection.
2. Add ClickHouse SQL dialect next to MySQL and PostgreSQL.
3. Validate response compatibility through `/api/ds/query`.
4. Prepare ClickHouse schema and numeric replication path on the test stand.
5. Run side-by-side comparisons against the regular Zabbix datasource.

## Non-goals for the first iteration

- replacing the Zabbix API
- replacing the Zabbix server database
- migrating string or log history
- background replication logic inside the Grafana plugin itself
