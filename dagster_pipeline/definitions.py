from dagster import Definitions

from dagster_pipeline.assets.producer_asset import run_producer
from dagster_pipeline.schedules.weather_schedule import weather_schedule
from dagster_pipeline.jobs.weather_job import weather_job


defs = Definitions(
    assets=[run_producer],
    jobs=[weather_job],
    schedules=[weather_schedule]
    )

