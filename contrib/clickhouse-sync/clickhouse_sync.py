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
        return ("itemid", "clock", "ns")
    return ("itemid", "clock")


def table_value_columns(table):
    if table in ("history", "history_uint"):
        return ("itemid", "clock", "ns", "value")
    return ("itemid", "clock", "num", "value_min", "value_avg", "value_max")


def build_cursor_clause(table, cursor):
    if not cursor:
        return ""
    if table in ("history", "history_uint"):
        return (
            "AND (itemid > {itemid} OR "
            "(itemid = {itemid} AND clock > {clock}) OR "
            "(itemid = {itemid} AND clock = {clock} AND ns > {ns}))"
        ).format(itemid=int(cursor["itemid"]), clock=int(cursor["clock"]), ns=int(cursor["ns"]))
    return (
        "AND (itemid > {itemid} OR "
        "(itemid = {itemid} AND clock > {clock}))"
    ).format(itemid=int(cursor["itemid"]), clock=int(cursor["clock"]))


def build_select_sql(table, start_clock, end_clock, itemid_start, itemid_end, batch_size, cursor):
    columns = ", ".join(table_value_columns(table))
    order_by = ", ".join(table_order_columns(table))
    cursor_clause = build_cursor_clause(table, cursor)
    return (
        f"SELECT {columns} "
        f"FROM {table} "
        f"WHERE itemid >= {int(itemid_start)} "
        f"AND itemid <= {int(itemid_end)} "
        f"AND clock >= {int(start_clock)} "
        f"AND clock <= {int(end_clock)} "
        f"{cursor_clause} "
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


def fetch_itemid_range(conn, source_type, table):
    sql = f"SELECT MIN(itemid) AS min_itemid, MAX(itemid) AS max_itemid FROM {table}"
    rows = fetch_rows(conn, source_type, sql)
    if not rows:
        return (None, None)
    row = rows[0]
    if row["min_itemid"] is None or row["max_itemid"] is None:
        return (None, None)
    return (int(row["min_itemid"]), int(row["max_itemid"]))


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
        {
            "backfill_completed": False,
            "backfill_next_itemid": None,
            "last_clock": None,
            "last_run_at": None,
        },
    )


def now_clock():
    return int(time.time())


def version_token():
    return time.time_ns()


def row_cursor(table, row):
    if table in ("history", "history_uint"):
        return {"itemid": int(row["itemid"]), "clock": int(row["clock"]), "ns": int(row["ns"])}
    return {"itemid": int(row["itemid"]), "clock": int(row["clock"])}


def emit_event(kind, **payload):
    record = {"event": kind, **payload}
    print(json.dumps(record, sort_keys=True), flush=True)


def run_slice(conn, config, state, table, start_clock, end_clock, itemid_start, itemid_end):
    source_type = config["source"]["type"]
    batch_size = int(config["sync"]["batch_size"])
    info = table_state(state, table)
    cursor = None
    total_rows = 0
    max_clock = info.get("last_clock") or 0
    while True:
        sql = build_select_sql(table, start_clock, end_clock, itemid_start, itemid_end, batch_size, cursor)
        rows = fetch_rows(conn, source_type, sql)
        if not rows:
            break
        clickhouse_insert(config, table, rows, version_token())
        total_rows += len(rows)
        last_row = rows[-1]
        cursor = row_cursor(table, last_row)
        max_clock = max(max_clock, int(last_row["clock"]))
        info["last_run_at"] = now_clock()
        save_state(config["state"]["file"], state)
        if len(rows) < batch_size:
            break
    if total_rows:
        emit_event(
            "slice_loaded",
            table=table,
            rows=total_rows,
            itemid_from=itemid_start,
            itemid_to=itemid_end,
            start_clock=start_clock,
            end_clock=end_clock,
        )
    return (total_rows, max_clock)


def iter_slices(min_itemid, max_itemid, slice_width, start_from=None):
    if min_itemid is None or max_itemid is None:
        return
    current = min_itemid if start_from is None else max(start_from, min_itemid)
    while current <= max_itemid:
        upper = min(current + slice_width - 1, max_itemid)
        yield (current, upper)
        current = upper + 1


def run_backfill(conn, config, state, tables):
    total = {}
    source_type = config["source"]["type"]
    start_clock = int(config["sync"].get("backfill_start_clock", 0))
    end_clock = now_clock()
    slice_width = int(config["sync"]["backfill_itemid_slice_width"])
    for table in tables:
        info = table_state(state, table)
        if info.get("backfill_completed"):
            total[table] = 0
            continue
        table_total = 0
        min_itemid, max_itemid = fetch_itemid_range(conn, source_type, table)
        if min_itemid is None:
            info["backfill_completed"] = True
            info["last_clock"] = end_clock
            save_state(config["state"]["file"], state)
            total[table] = 0
            continue
        next_itemid = info.get("backfill_next_itemid")
        for itemid_start, itemid_end in iter_slices(min_itemid, max_itemid, slice_width, next_itemid):
            rows, max_clock = run_slice(conn, config, state, table, start_clock, end_clock, itemid_start, itemid_end)
            table_total += rows
            info["backfill_next_itemid"] = itemid_end + 1
            info["last_clock"] = max_clock if max_clock else info.get("last_clock")
            save_state(config["state"]["file"], state)
        info["backfill_completed"] = True
        info["backfill_next_itemid"] = None
        info["last_clock"] = end_clock
        info["last_run_at"] = now_clock()
        save_state(config["state"]["file"], state)
        emit_event("table_backfill_completed", table=table, rows=table_total, end_clock=end_clock)
        total[table] = table_total
    return total


def run_recent_window(conn, config, state, tables, start_clock, end_clock, mode_name):
    total = {}
    source_type = config["source"]["type"]
    slice_width = int(config["sync"]["backfill_itemid_slice_width"])
    for table in tables:
        table_total = 0
        info = table_state(state, table)
        min_itemid, max_itemid = fetch_itemid_range(conn, source_type, table)
        if min_itemid is None:
            total[table] = 0
            continue
        for itemid_start, itemid_end in iter_slices(min_itemid, max_itemid, slice_width):
            rows, max_clock = run_slice(conn, config, state, table, start_clock, end_clock, itemid_start, itemid_end)
            table_total += rows
            if max_clock:
                info["last_clock"] = max(max_clock, info.get("last_clock") or 0)
                info["last_run_at"] = now_clock()
                save_state(config["state"]["file"], state)
        if mode_name == "catchup":
            info["last_clock"] = end_clock
            info["last_run_at"] = now_clock()
            save_state(config["state"]["file"], state)
        emit_event("table_window_completed", mode=mode_name, table=table, rows=table_total, start_clock=start_clock, end_clock=end_clock)
        total[table] = table_total
    return total


def run_catchup(conn, config, state, tables):
    overlap = int(config["sync"]["overlap_seconds"])
    end_clock = now_clock()
    start_clocks = {}
    for table in tables:
        info = table_state(state, table)
        last_clock = info.get("last_clock")
        if last_clock is None:
            start_clocks[table] = int(config["sync"].get("backfill_start_clock", 0))
        else:
            start_clocks[table] = max(0, int(last_clock) - overlap)
    result = {}
    for table in tables:
        result.update(run_recent_window(conn, config, state, [table], start_clocks[table], end_clock, "catchup"))
    return result


def run_repair(conn, config, state, tables):
    window = int(config["sync"]["repair_window_seconds"])
    end_clock = now_clock()
    start_clock = max(0, end_clock - window)
    return run_recent_window(conn, config, state, tables, start_clock, end_clock, "repair")


def print_result(mode, result):
    output = {"mode": mode, "tables": result, "finished_at": now_clock()}
    print(json.dumps(output, sort_keys=True), flush=True)


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
