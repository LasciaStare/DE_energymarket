CREATE SCHEMA silver;
CREATE SCHEMA gold;
CREATE SCHEMA ops;

CREATE TABLE ops.pipeline_runs (
    run_id uuid PRIMARY KEY,
    metric text NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status text NOT NULL CHECK (status IN ('running', 'success', 'partial', 'failed')),
    accepted integer NOT NULL DEFAULT 0,
    rejected integer NOT NULL DEFAULT 0,
    detail text
);
CREATE INDEX pipeline_runs_recent ON ops.pipeline_runs (started_at DESC);
CREATE TABLE ops.quarantine (
    run_id uuid NOT NULL REFERENCES ops.pipeline_runs,
    day date,
    reason text NOT NULL
);
CREATE VIEW ops.dq_runs AS
SELECT run_id, metric, start_date, end_date, status, accepted, rejected
FROM ops.pipeline_runs;

CREATE TABLE silver.observation (
    metric text NOT NULL,
    day date NOT NULL,
    hour smallint NOT NULL CHECK (hour BETWEEN 0 AND 24),
    entity text NOT NULL,
    technology text NOT NULL,
    value double precision NOT NULL CHECK (value >= 0 AND value < 'Infinity'::double precision),
    run_id uuid NOT NULL REFERENCES ops.pipeline_runs,
    PRIMARY KEY (metric, day, hour, entity)
);

CREATE VIEW gold.market_daily AS
SELECT day,
       avg(value) FILTER (WHERE metric = 'price') AS price_cop_kwh,
       sum(value) FILTER (WHERE metric = 'demand') / 1000000 AS demand_gwh,
       max(value) FILTER (WHERE metric = 'reservoir') * 100 AS reservoir_pct
FROM silver.observation
WHERE metric IN ('price', 'demand', 'reservoir')
GROUP BY day;

CREATE VIEW gold.generation_mix AS
SELECT day, technology, sum(value) / 1000000 AS generation_gwh
FROM silver.observation
WHERE metric = 'generation'
GROUP BY day, technology;

CREATE VIEW gold.price_hourly AS
SELECT day, hour, value AS price_cop_kwh
FROM silver.observation
WHERE metric = 'price';

CREATE VIEW gold.freshness AS
SELECT metric, max(day) AS latest_day FROM silver.observation GROUP BY metric;

GRANT USAGE ON SCHEMA silver, ops TO market_loader;
GRANT SELECT, INSERT, DELETE ON silver.observation TO market_loader;
GRANT SELECT, INSERT, UPDATE ON ops.pipeline_runs TO market_loader;
GRANT INSERT ON ops.quarantine TO market_loader;
GRANT USAGE ON SCHEMA gold, ops TO market_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO market_reader;
GRANT SELECT ON ops.pipeline_runs, ops.dq_runs TO market_reader;
