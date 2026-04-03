from dagster import define_asset_job
from dagster_pipeline.assets.producer_asset import run_producer


weather_job = define_asset_job(name = "weather_pipeline_job",
                               selection=[run_producer]
                               )




