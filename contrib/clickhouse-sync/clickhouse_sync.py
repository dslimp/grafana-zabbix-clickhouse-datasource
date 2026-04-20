#!/usr/bin/env python3

import argparse
import base64
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

import yaml


NUMERIC_TABLES = ("history", "history_uint", "trends", "trends_uint")


def load_mysql_driver():
    try:
        import pymysql
    except ImportError as exc:
        raise RuntimeError("pymysql is required for mysql source mode") from exc
    return pymysql


def load_postgres_driver():
    try:
        import psycopg
        return ("psycopg", psycopg)
    except ImportError:
        pass
    try:
        import psycopg2
        return ("psycopg2", psycopg2)
    except ImportError as exc:
        raise RuntimeError("psycopg or psycopg2 is required for postgres source mode") from exc


def load_config(path):
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def ensure_state(path):
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    if not state_path.exists():
        state = {"tables": {}}
        save_state(path, state)
        return state
    with open(state_path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def save_state(path, state):
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = state_path.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, sort_keys=True)
    tmp_path.replace(state_path)


def source_connect(config):
    source = config["source"]
    if source["type"] == "mysql":
        driver = load_mysql_driver()
        return driver.connect(
            host=source["host"],
            port=int(source["port"]),
            user=source["user"],
            password=source["password"],
            database=source["database"],
            cursorclass=driver.cursors.DictCursor,
            autocommit=True,
        )
    if source["type"] == "postgres":
        driver_name, driver = load_postgres_driver()
        if driver_name == "psycopg":
            return driver.connect(
                host=source["host"],
                port=int(source["port"]),
                user=source["user"],
                password=source["password"],
                dbname=source["database"],
                autocommit=True,
                row_factory=driver.rows.dict_row,
            )
        conn = driver.connect(
            host=source["host"],
            port=int(source["port"]),
            user=source["user"],
            password=source["password"],
            dbname=source["database"],
        )
        conn.autocommit = True
        return conn
    raise RuntimeError(f"unsupported source type: {source['type']}")


def table_order_columns(table):
    if table in ("history", "history_uint"):
        return ("clock", "itemid", "ns")
    return ("clock", "itemid")


def table_value_columns(table):
    if table in ("history", "history_uint"):
        return ("itemid", "clock", "ns", "value")
    return ("itemid", "clock", "num", "value_min", "value_avg", "value_max")


def build_window_clause(start_clock, end_clock):
    return f"clock >= {int(start_clock)} AND clock <= {int(end_clock)}"


def build_cursor_clause(table, cursor):
    if not cursor:
        return ""
    if table in ("history", "history_uint"):
        return (
            "AND (clock > {clock} OR "
            "(clock = {clock} AND itemid > {itemid}) OR "
            "(clock = {clock} AND itemid = {itemid} AND ns > {ns}))"
        ).format(clock=int(cursor["clock"]), itemid=int(cursor["itemid"]), ns=int(cursor["ns"]))
    return (
        "AND (clock > {clock} OR "
        "(clock = {clock} AND itemid > {itemid}))"
    ).format(clock=int(cursor["clock"]), itemid=int(cursor["itemid"]))


def build_select_sql(table, start_clock, end_clock, batch_size, cursor):
    columns = ", ".join(table_value_columns(table))
    order_by = ", ".join(table_order_columns(table))
    where = build_window_clause(start_clock, end_clock)
    cursor_clause = build_cursor_clause(table, cursor)
    return (
        f"SELECT {columns} "
        f"FROM {table} "
        f"WHERE {where} {cursor_clause} "
        f"ORDER BY {order_by} "
        f"LIMIT {int(batch_size)}"
    )


def fetch_rows(conn, source_type, sql):
    if source_type == "mysql":
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        if rows and not isinstance(rows[0], dict):
            columns = [col.name if hasattr(col, "name") else col[0] for col in cur.description]
            return [dict(zip(columns, row)) for row in rows]
        return rows


def clickhouse_headers(config):
    user = config["clickhouse"].get("user", "")
    password = config["clickhouse"].get("password", "")
    headers = {"Content-Type": "application/json"}
    if user:
        raw = f"{user}:{password}".encode("utf-8")
        headers["Authorization"] = "Basic " + base64.b64encode(raw).decode("ascii")
    return headers


def clickhouse_url(config, query):
    target = config["clickhouse"]
    params = urllib.parse.urlencode({"query": query, "database": target["database"]})
    return f"{target['scheme']}://{target['host']}:{target['port']}/?{params}"


def clickhouse_insert(config, table, rows, version):
    payload_rows = []
    for row in rows:
        item = dict(row)
        item["_version"] = version
        payload_rows.append(json.dumps(item, separators=(",", ":")))
    payload = ("\n".join(payload_rows) + "\n").encode("utf-8")
    query = f"INSERT INTO {table} FORMAT JSONEachRow"
    req = urllib.request.Request(
        clickhouse_url(config, query),
        data=payload,
        headers=clickhouse_headers(config),
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=300) as response:
        response.read()


def table_state(state, table):
    return state.setdefault("tables", {}).setdefault(
        table,
        {"backfill_completed": False, "last_clock": None, "cursor": None, "last_run_at": None},
    )


def reset_cursor(table_info):
    table_info["cursor"] = None


def now_clock():
    return int(time.time())


def version_token():
    return time.time_ns()


def row_cursor(table, row):
    if table in ("history", "history_uint"):
        return {"clock": int(row["clock"]), "itemid": int(row["itemid"]), "ns": int(row["ns"])}
    return {"clock": int(row["clock"]), "itemid": int(row["itemid"])}


def run_window(conn, config, state, table, start_clock, end_clock):
    source_type = config["source"]["type"]
    batch_size = int(config["sync"]["batch_size"])
    info = table_state(state, table)
    total_rows = 0
    while True:
        sql = build_select_sql(table, start_clock, end_clock, batch_size, info.get("cursor"))
        rows = fetch_rows(conn, source_type, sql)
        if not rows:
            break
        clickhouse_insert(config, table, rows, version_token())
        total_rows += len(rows)
        last_row = rows[-1]
        info["cursor"] = row_cursor(table, last_row)
        info["last_clock"] = int(last_row["clock"])
        info["last_run_at"] = now_clock()
        save_state(config["state"]["file"], state)
        if len(rows) < batch_size:
            break
    return total_rows


def run_backfill(conn, config, state, tables):
    total = {}
    start_clock = int(config["sync"].get("backfill_start_clock", 0))
    end_clock = now_clock()
    for table in tables:
        info = table_state(state, table)
        if info.get("backfill_completed"):
            total[table] = 0
            continue
        if info.get("last_clock") is None:
            reset_cursor(info)
        total[table] = run_window(conn, config, state, table, start_clock, end_clock)
        info["backfill_completed"] = True
        reset_cursor(info)
        save_state(config["state"]["file"], state)
    return total


def run_catchup(conn, config, state, tables):
    total = {}
    overlap = int(config["sync"]["overlap_seconds"])
    end_clock = now_clock()
    for table in tables:
        info = table_state(state, table)
        last_clock = info.get("last_clock")
        if last_clock is None:
            start_clock = int(config["sync"].get("backfill_start_clock", 0))
        else:
            start_clock = max(0, int(last_clock) - overlap)
        reset_cursor(info)
        total[table] = run_window(conn, config, state, table, start_clock, end_clock)
        reset_cursor(info)
        save_state(config["state"]["file"], state)
    return total


def run_repair(conn, config, state, tables):
    total = {}
    window = int(config["sync"]["repair_window_seconds"])
    end_clock = now_clock()
    start_clock = max(0, end_clock - window)
    for table in tables:
        info = table_state(state, table)
        reset_cursor(info)
        total[table] = run_window(conn, config, state, table, start_clock, end_clock)
        reset_cursor(info)
        save_state(config["state"]["file"], state)
    return total


def print_result(mode, result):
    output = {"mode": mode, "tables": result, "finished_at": now_clock()}
    print(json.dumps(output, sort_keys=True))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--mode", choices=("backfill", "catchup", "repair", "daemon"), required=True)
    parser.add_argument("--tables", nargs="*", default=list(NUMERIC_TABLES))
    return parser.parse_args()


def validate_tables(tables):
    invalid = sorted(set(tables) - set(NUMERIC_TABLES))
    if invalid:
        raise RuntimeError(f"unsupported tables requested: {', '.join(invalid)}")


def daemon_loop(conn, config, state, tables):
    sleep_seconds = int(config["sync"]["sleep_seconds"])
    repair_every = int(config["sync"]["repair_every"])
    iteration = 0
    while True:
        iteration += 1
        catchup_result = run_catchup(conn, config, state, tables)
        print_result("catchup", catchup_result)
        if repair_every > 0 and iteration % repair_every == 0:
            repair_result = run_repair(conn, config, state, tables)
            print_result("repair", repair_result)
        time.sleep(sleep_seconds)


def main():
    args = parse_args()
    config = load_config(args.config)
    validate_tables(args.tables)
    state = ensure_state(config["state"]["file"])
    conn = source_connect(config)
    try:
        if args.mode == "backfill":
            print_result("backfill", run_backfill(conn, config, state, args.tables))
            return 0
        if args.mode == "catchup":
            print_result("catchup", run_catchup(conn, config, state, args.tables))
            return 0
        if args.mode == "repair":
            print_result("repair", run_repair(conn, config, state, args.tables))
            return 0
        daemon_loop(conn, config, state, args.tables)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
