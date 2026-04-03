from confluent_kafka import Consumer
import os
from dotenv import load_dotenv
from pymongo import MongoClient
import logging
import json

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

def run_consumer():
    consumer.subscribe(['weather_events'])
    log.info("Kafka consumer subscribed to topic 'weather_events'")
    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                log.error(f"Consumer error: {msg.error()}")
                continue
            data = json.loads(msg.value().decode('utf-8'))
            log.info(f"Received weather event: {data['city']} at {data['timestamp']}")

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

if __name__ == "__main__":
    run_consumer()            
