import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from psycopg.rows import dict_row

app = FastAPI(title="Pulso Eléctrico Colombia", version="1.0")


def query(sql, params=()):
    try:
        with psycopg.connect(row_factory=dict_row, connect_timeout=5,
                             options="-c statement_timeout=5000") as conn:
            return conn.execute(sql, params).fetchall()
    except psycopg.Error as exc:
        logging.error("Database unavailable: %s", type(exc).__name__)
        raise HTTPException(503, "Datos temporalmente no disponibles") from exc


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(Path(__file__).with_name("dashboard.html"), media_type="text/html")


@app.get("/health")
def health():
    query("SELECT 1")
    return {"status": "ok"}


@app.get("/v1/market")
def market(start: date | None = None, end: date | None = None):
    end = end or datetime.now(ZoneInfo("America/Bogota")).date() - timedelta(days=1)
    start = start or end - timedelta(days=29)
    if not 0 <= (end - start).days <= 365:
        raise HTTPException(422, "Usa un intervalo ascendente de hasta 366 días.")
    params = (start, end)
    return {
        "daily": query("SELECT * FROM gold.market_daily WHERE day BETWEEN %s AND %s ORDER BY day", params),
        "generation": query("SELECT * FROM gold.generation_mix WHERE day BETWEEN %s AND %s "
                            "ORDER BY day, technology", params),
        "spikes": query(
            "WITH sample AS (SELECT * FROM gold.price_hourly WHERE day BETWEEN %s AND %s), "
            "limits AS (SELECT hour, percentile_cont(.25) WITHIN GROUP (ORDER BY price_cop_kwh) AS q1, "
            "percentile_cont(.75) WITHIN GROUP (ORDER BY price_cop_kwh) AS q3 "
            "FROM sample GROUP BY hour HAVING count(*) >= 14) "
            "SELECT day, hour, price_cop_kwh, q3 + 1.5 * (q3 - q1) AS threshold "
            "FROM sample JOIN limits USING (hour) WHERE price_cop_kwh > q3 + 1.5 * (q3 - q1) "
            "ORDER BY price_cop_kwh DESC, day, hour LIMIT 30", params),
    }


@app.get("/v1/operations")
def operations():
    return {
        "freshness": query("SELECT metric, latest_day, "
                           "(now() AT TIME ZONE 'America/Bogota')::date - latest_day AS age_days "
                           "FROM gold.freshness ORDER BY metric"),
        "runs": query("SELECT run_id, metric, start_date, end_date, started_at, finished_at, status, "
                      "accepted, rejected, detail FROM ops.pipeline_runs ORDER BY started_at DESC LIMIT 50"),
    }
