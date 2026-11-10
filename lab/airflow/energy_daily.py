"""Airflow 3 alternative to the Compose scheduler, on a separate worker."""
from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="energy_market_daily",
    schedule="0 13 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/Bogota"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
)
def energy_daily():
    @task
    def ingest():
        from pipeline import METRICS, load

        end = get_current_context()["data_interval_end"].in_timezone("America/Bogota").date()
        if not load(end - timedelta(days=7), end - timedelta(days=1), METRICS):
            raise RuntimeError("Carga incompleta. Consultar ops.pipeline_runs.")

    ingest()


energy_daily()
