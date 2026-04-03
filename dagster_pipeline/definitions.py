from dagster import Definitions

from dagster_pipeline.assets.ingestion_cycle_asset import run_ingestion_cycle
from dagster_pipeline.schedules.weather_schedule import weather_schedule
from dagster_pipeline.jobs.weather_job import weather_job


defs = Definitions(
    assets=[run_ingestion_cycle],
    jobs=[weather_job],
    schedules=[weather_schedule]
    )

