# Weather Ingestion Pipeline

This project is a focused data ingestion pipeline that collects live weather data from the Open-Meteo API, publishes each validated weather event to Kafka, and persists the consumed records into MongoDB. Dagster is used as the orchestration layer to run the ingestion cycle on a schedule.

The repository has been intentionally trimmed to the final implemented scope. It now represents only the ingestion side of the larger lakehouse idea, without downstream transformation, quality, monitoring, or serving layers.

## Architecture

The active flow in this repository is:

`Dagster -> Python Producer -> Kafka -> Python Consumer -> MongoDB`

### Component Roles

- `Dagster` triggers and coordinates one bounded ingestion cycle.
- `Producer` fetches weather data for configured cities from Open-Meteo.
- `Pydantic` validates the payload before publishing.
- `Kafka` acts as the event transport layer between producer and consumer.
- `Consumer` reads Kafka messages and upserts them into MongoDB.
- `MongoDB` stores the final ingested weather events.

## Project Structure

```text
dagster_pipeline/
  assets/
    ingestion_cycle_asset.py
  jobs/
    weather_job.py
  schedules/
    weather_schedule.py
  definitions.py

ingestion/
  producer.py
  consumer.py
  schemas.py

kafka/
  create_topics.py
```

## How It Works

1. Dagster starts the ingestion asset.
2. The asset launches the Kafka consumer first so it is ready to receive events.
3. The producer calls the weather API for each configured city.
4. Each response is validated against the `WeatherEvent` schema.
5. Valid events are published to the Kafka topic `weather_events`.
6. The consumer reads those messages and performs MongoDB upserts using `city + timestamp` as a unique key.
7. The consumer exits after a bounded idle timeout, allowing the Dagster run to finish cleanly.

## Features

- Scheduled orchestration with Dagster
- API ingestion from Open-Meteo
- Event-based transfer through Kafka
- Schema validation with Pydantic
- MongoDB upsert-based persistence
- Bounded producer/consumer cycle suitable for scheduled runs

## Environment Variables

This project reads configuration from a local `.env` file. A safe sample is included in `.env.example`.

Expected variables:

- `OPEN_METEO_BASE_URL`
- `WEATHER_CITIES`
- `KAFKA_BOOTSTRAP_SERVERS`
- `MONGO_ATLAS_URI`
- `MONGO_DB_NAME`
- `MONGO_COLLECTION`

## Running The Project

Install dependencies:

```bash
pip install -r requirements.txt
```

Before running Dagster, start the Kafka server first.

If you are using Kafka in KRaft mode on Windows:

```cmd
cd D:\kafka_2.13-4.2.0
bin\windows\kafka-server-start.bat config\kraft\server.properties
```

Create Kafka topic:

```bash
python kafka/create_topics.py
```

Run the producer manually:

```bash
python ingestion/producer.py
```

Run the consumer manually:

```bash
python ingestion/consumer.py
```

Run through Dagster:

```bash
dagster dev
```

Recommended startup order:

1. Start Kafka server
2. Create Kafka topic
3. Run Dagster

## Dagster Configuration

- Dagster module: `dagster_pipeline.definitions`
- Job: `weather_pipeline_job`
- Schedule: every 30 minutes
- Main asset: `run_ingestion_cycle`

## Kafka Configuration

- Topic name: `weather_events`
- Number of partitions: `1`
- Replication factor: `1`
- Consumer group ID: `weather_consumer_group_test`

## Data Model

Each ingested weather event contains:

- `event_id`
- `city`
- `latitude`
- `longitude`
- `temperature_2m`
- `relative_humidity_2m`
- `wind_speed_10m`
- `precipitation`
- `timestamp`

## Security Notes

- Real credentials are expected only in the local `.env` file.
- `.env` is already listed in `.gitignore` and is not currently tracked by git.
- No hardcoded MongoDB URI, password, token, or API key was found in the tracked project files during the sweep I ran.
- Use `.env.example` in GitHub instead of uploading your real `.env`.

If you ever committed secrets in an earlier commit, they are still recoverable from git history even if the current files are clean. In that case, rotate those keys and clean the history before publishing.

## Tech Stack

- Python
- Dagster
- Kafka
- MongoDB
- Pydantic
- Requests
- python-dotenv

## Current Scope

This repository currently covers ingestion only. It does not include:

- dbt transformations
- data quality tools such as Great Expectations
- monitoring dashboards
- downstream analytics or serving layers

That keeps the repo aligned with the implementation that actually exists today.
