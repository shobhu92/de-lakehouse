import json
import logging
import os

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

from schemas import WeatherEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/pipeline.log")
    ]
)

log = logging.getLogger(__name__)
handler = logging.FileHandler("pipeline.log")
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
log.addHandler(handler)

load_dotenv()

cities = json.loads(os.getenv("WEATHER_CITIES"))
producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS")})
delivery_failures = []


def delivery_report(err, msg):
    if err is not None:
        delivery_failures.append(str(err))
        log.error(f"Kafka delivery failed for topic {msg.topic()}: {err}")
    else:
        log.info(
            f"Kafka delivery confirmed for {msg.topic()} "
            f"[partition={msg.partition()}, offset={msg.offset()}]"
        )


def fetch_city_weather(city):
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
    }
    endpoint = os.getenv("OPEN_METEO_BASE_URL")
    resp = requests.get(endpoint, params=params, timeout=10)
    if resp.status_code == 200:
        data = resp.json()
        log.info(f"Successfully fetched weather data for {city['name']}")
        return data

    log.warning(f"Failed to fetch weather data for {city['name']}")
    return None


def parse_and_validate(fetched_weather, city):
    current = fetched_weather.get("current", {})

    event = WeatherEvent(
        city=city["name"],
        latitude=city["latitude"],
        longitude=city["longitude"],
        temperature_2m=current.get("temperature_2m"),
        relative_humidity_2m=current.get("relative_humidity_2m"),
        wind_speed_10m=current.get("wind_speed_10m"),
        precipitation=current.get("precipitation", 0.0),
        timestamp=current.get("time"),
    )

    log.info("Weather data validated successfully")
    return event

def run_producer():
    produced_count = 0
    delivery_failures.clear()

    for city in cities:
        result = fetch_city_weather(city)
        if result:
            resulted_data = parse_and_validate(result, city)
            if resulted_data:
                producer.produce(
                    "weather_events",
                    json.dumps(resulted_data.to_mongo_dict()).encode("utf-8"),
                    callback=delivery_report,
                )
                producer.poll(0)
                log.info(f"Weather event for {city['name']} queued for Kafka delivery")
                produced_count += 1
    pending_messages = producer.flush()
    if pending_messages:
        log.warning(f"{pending_messages} Kafka message(s) were not delivered before shutdown")

    if delivery_failures:
        raise RuntimeError(
            "Kafka delivery failed for one or more messages. "
            f"bootstrap_servers={os.getenv('KAFKA_BOOTSTRAP_SERVERS')}, "
            f"failures={delivery_failures}"
        )

    if pending_messages == 0:
        log.info("All queued Kafka messages were delivered successfully")
    log.info(f"Kafka producer queued {produced_count} weather event(s) in total")
    return produced_count

if __name__ == "__main__":
    run_producer()
