## DE Lakehouse Final Scope

This repo now contains only the ingestion pipeline:

- Dagster schedule/job/asset orchestration
- Weather API producer
- Kafka transport
- MongoDB consumer storage

Main flow: `Dagster -> producer -> Kafka -> consumer -> MongoDB`
