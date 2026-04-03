from dagster import ScheduleDefinition
from dagster_pipeline.jobs.weather_job import weather_job



weather_schedule = ScheduleDefinition(
    job=weather_job,
    cron_schedule="*/30 * * * *"
)

