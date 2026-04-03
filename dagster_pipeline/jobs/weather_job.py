from dagster import define_asset_job, AssetSelection
from dagster_pipeline.assets.ingestion_cycle_asset import run_ingestion_cycle


weather_job = define_asset_job(name = "weather_pipeline_job",
                               selection=AssetSelection.assets(run_ingestion_cycle))







