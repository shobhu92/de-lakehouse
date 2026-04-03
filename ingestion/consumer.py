from confluent_kafka import Consumer
import os
from dotenv import load_dotenv
from pymongo import MongoClient
import logging
import json
import argparse
import time

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

log = logging.getLogger(__name__)
consumer = Consumer({
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    'group.id': 'weather_consumer_group_test',
    'auto.offset.reset': 'earliest'})

#MongoDB connection:

mongo_url = os.getenv("MONGO_ATLAS_URI")
client = MongoClient(mongo_url)
db = client[os.getenv("MONGO_DB_NAME")]
collection = db[os.getenv("MONGO_COLLECTION")]
collection.create_index(
    [("city", 1), ("timestamp", 1)],
    unique=True
)

def run_consumer(startup_timeout=30, idle_timeout=5):
    consumer.subscribe(['weather_events'])
    log.info("Kafka consumer subscribed to topic 'weather_events'")
    consumed_count = 0
    start_time = time.monotonic()
    last_message_time = None

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                now = time.monotonic()
                if consumed_count == 0 and now - start_time >= startup_timeout:
                    log.info("No Kafka messages received before startup timeout; consumer exiting")
                    break
                if last_message_time is not None and now - last_message_time >= idle_timeout:
                    log.info("No new Kafka messages received within idle timeout; consumer exiting")
                    break
                continue
            if msg.error():
                log.error(f"Consumer error: {msg.error()}")
                continue
            data = json.loads(msg.value().decode('utf-8'))
            log.info(f"Received weather event: {data['city']} at {data['timestamp']}")
            consumed_count += 1
            last_message_time = time.monotonic()

            try:
                collection.update_one(
                    {"city": data["city"], "timestamp": data["timestamp"]},
                    {"$set": data},
                    upsert=True
                )
                log.info(f"Weather event for {data['city']} inserted/updated in MongoDB successfully")
            except Exception as e:
                log.error(f"Error occurred while inserting/updating data into MongoDB: {e}")
    except KeyboardInterrupt:
        log.info("Kafka consumer stopped by user")
    finally:
        consumer.close()
    log.info(f"Kafka consumer processed {consumed_count} message(s) before exit")
    return consumed_count


def parse_args():
    parser = argparse.ArgumentParser(description="Consume weather events from Kafka into MongoDB.")
    parser.add_argument(
        "--startup-timeout",
        type=int,
        default=30,
        help="Seconds to wait for the first Kafka message before exiting."
    )
    parser.add_argument(
        "--idle-timeout",
        type=int,
        default=5,
        help="Seconds to wait after the last Kafka message before exiting."
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    run_consumer(
        startup_timeout=args.startup_timeout,
        idle_timeout=args.idle_timeout
    )
