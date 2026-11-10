"""Extract XM and SIMEM into a local lake and PostgreSQL."""
import argparse
import gzip
import json
import logging
import math
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from shutil import disk_usage
from time import sleep
from uuid import uuid4
from zoneinfo import ZoneInfo

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import yaml

ROOT = Path(__file__).resolve().parent
METRICS = yaml.safe_load((ROOT / "metrics.yml").read_text())
LAKE = Path(os.getenv("LAKE_DIR", "lake"))
BOGOTA = ZoneInfo("America/Bogota")
DAY = timedelta(days=1)
SCHEMA = pa.schema([
    ("metric", pa.string()), ("day", pa.date32()), ("hour", pa.int16()),
    ("entity", pa.string()), ("technology", pa.string()), ("value", pa.float64()),
])


def days(start, end):
    while start <= end:
        yield start
        start += DAY


def windows(start, end):
    while start <= end:
        stop = min(end, start + 29 * DAY)
        yield start, stop
        start = stop + DAY


def fetch(config, start, end):
    for attempt in range(3):
        try:
            if config["source"] == "xm":
                response = requests.post(
                    f"https://servapibi.xm.com.co/{config['period']}",
                    json={"MetricId": config["id"], "Entity": "Sistema",
                          "StartDate": str(start), "EndDate": str(end), "Filter": []},
                    timeout=(10, 90),
                )
            else:
                response = requests.get(
                    "https://www.simem.co/backend-files/api/PublicData",
                    params={"datasetid": config["id"], "startDate": str(start),
                            "endDate": str(end)}, timeout=(10, 90),
                )
            if response.status_code in (408, 429) or response.status_code >= 500:
                response.raise_for_status()
            break
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError):
            if attempt == 2:
                raise
            sleep(2 ** (attempt + 1))
    return response


def normalize(metric, config, payload, start, end):
    """Reject an entire day when its observations cannot be safely replaced."""
    hourly = config["period"] == "hourly"
    simem = config["source"] == "simem"
    if simem and payload["success"] is not True:
        raise ValueError("SIMEM returned an unsuccessful response")
    groups = defaultdict(list)
    records = payload["result"]["records"] if simem else payload["Items"]
    for item in records:
        day = date.fromisoformat(item["Fecha" if simem else "Date"])
        if not start <= day <= end:
            raise ValueError(f"Unexpected date: {day}")
        if simem:
            groups[day].append(item)
        else:
            groups[day].extend(item["HourlyEntities" if hourly else "DailyEntities"])

    accepted, rejected = {}, {}
    for day in days(start, end):
        try:
            items = groups[day]
            if not items:
                raise ValueError("Día sin datos")
            if not simem and len(items) != 1:
                raise ValueError("Se esperaba un registro del sistema")
            rows, keys = [], set()
            for item in items:
                if simem:
                    entity = item["CodigoPlanta"]
                    technology = item["TipoGeneracion"]
                    if not isinstance(entity, str) or not entity.strip():
                        raise ValueError("Código de planta vacío")
                    if not isinstance(technology, str) or not technology.strip():
                        raise ValueError("Tecnología vacía")
                    values = [(0, item["GeneracionRealEstimada"])]
                elif hourly:
                    entity, technology = "SIN", ""
                    values = [(h, item["Values"][f"Hour{h:02}"]) for h in range(1, 25)]
                else:
                    entity, technology = "SIN", ""
                    values = [(0, item["Value"])]
                for hour, raw in values:
                    value = float(raw)
                    if not math.isfinite(value) or not 0 <= value <= config["max_value"]:
                        raise ValueError("Valor fuera del rango configurado")
                    key = (entity, hour)
                    if key in keys:
                        raise ValueError("Clave duplicada")
                    keys.add(key)
                    rows.append(dict(zip(SCHEMA.names, (metric, day, hour, entity, technology, value))))
            accepted[day] = rows
        except (KeyError, ValueError, TypeError, OverflowError) as exc:
            rejected[day] = str(exc)
    return accepted, rejected


def archive(path, write):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    try:
        write(temporary)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def run(conn, metric, start, end):
    config, run_id = METRICS[metric], uuid4()
    conn.execute(
        "INSERT INTO ops.pipeline_runs (run_id, metric, start_date, end_date, status) "
        "VALUES (%s, %s, %s, %s, 'running')", (run_id, metric, start, end),
    )
    bronze = LAKE / "bronze" / f"metric={metric}" / f"ingested={datetime.now(BOGOTA).date()}"
    try:
        LAKE.mkdir(parents=True, exist_ok=True)
        if disk_usage(LAKE).free < 2 * 1024**3:
            raise OSError("Quedan menos de 2 GiB libres en el volumen del lago")
        response = fetch(config, start, end)
        archive(bronze / f"{run_id}.json.gz",
                lambda path: path.write_bytes(gzip.compress(response.content)))
        response.raise_for_status()
        accepted, rejected = normalize(metric, config, response.json(), start, end)
        for day, rows in accepted.items():
            path = LAKE / "silver" / f"metric={metric}" / f"day={day}" / f"{run_id}.parquet"
            archive(path, lambda tmp: pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), tmp))

        status = "partial" if rejected and accepted else "failed" if rejected else "success"
        count = sum(map(len, accepted.values()))
        with conn.transaction():
            with conn.cursor() as cur:
                for day, rows in accepted.items():
                    cur.execute("DELETE FROM silver.observation WHERE metric = %s AND day = %s",
                                (metric, day))
                    with cur.copy("COPY silver.observation "
                                  "(metric, day, hour, entity, technology, value, run_id) FROM STDIN") as copy:
                        for row in rows:
                            copy.write_row((*row.values(), run_id))
                cur.executemany("INSERT INTO ops.quarantine VALUES (%s, %s, %s)",
                                [(run_id, day, reason) for day, reason in rejected.items()])
                cur.execute(
                    "UPDATE ops.pipeline_runs SET finished_at = now(), status = %s, "
                    "accepted = %s, rejected = %s WHERE run_id = %s",
                    (status, count, len(rejected), run_id),
                )
        logging.info("%s %s..%s %s (%d rows, %d rejected days)", metric, start, end,
                     status, count, len(rejected))
        return status == "success"
    except Exception as exc:
        logging.exception("%s failed (%s)", metric, run_id)
        with conn.transaction():
            conn.execute("INSERT INTO ops.quarantine VALUES (%s, NULL, %s)",
                         (run_id, str(exc)[:500]))
            conn.execute("UPDATE ops.pipeline_runs SET finished_at = now(), status = 'failed', "
                         "detail = %s WHERE run_id = %s", (type(exc).__name__, run_id))
        return False


def load(start, end, metrics):
    results = []
    with psycopg.connect(autocommit=True, connect_timeout=10) as conn:
        if not conn.execute("SELECT pg_try_advisory_lock(792050)").fetchone()[0]:
            logging.warning("Otra carga está en ejecución")
            return False
        conn.execute("UPDATE ops.pipeline_runs SET status = 'failed', finished_at = now(), "
                     "detail = 'Interrumpida' WHERE status = 'running'")
        for metric in metrics:
            # Smaller generation windows keep SIMEM response and memory bounded.
            for first, last in windows(start, end):
                spans = ((d, d) for d in days(first, last)) if metric == "generation" else [(first, last)]
                for low, high in spans:
                    results.append(run(conn, metric, low, high))
    return all(results)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--metric", action="append", choices=METRICS)
    parser.add_argument("--schedule", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    metrics = args.metric or METRICS
    if args.schedule:
        if args.start or args.end:
            parser.error("--schedule cannot be combined with explicit dates")
        last_attempt = None
        while True:
            now = datetime.now(BOGOTA)
            if now.hour >= 13 and now.date() != last_attempt:
                last_attempt = now.date()
                try:
                    # Revisit a full week to collect delayed XM publications.
                    load(last_attempt - 7 * DAY, last_attempt - DAY, metrics)
                except (OSError, psycopg.Error):
                    logging.exception("Scheduled run could not complete")
            sleep(60)
    if not args.start or not args.end or args.start > args.end:
        parser.error("Provide --start and --end in ascending order")
    return 0 if load(args.start, args.end, metrics) else 1


if __name__ == "__main__":
    raise SystemExit(main())
