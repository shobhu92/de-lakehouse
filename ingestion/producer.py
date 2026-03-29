from fastapi import params
import requests
import json
import logging
import configparser
from dotenv import load_dotenv
import os
from pymongo import MongoClient
from schemas import WeatherEvent
from pymongo.errors import DuplicateKeyError

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S"
    )

log = logging.getLogger(__name__)
dead_letter = logging.getLogger("dead_log")
dead_letter.setLevel(logging.WARNING)
fh = logging.FileHandler("dead_letter.log")
fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
dead_letter.addHandler(fh) 

load_dotenv()

url = os.getenv("MONGO_ATLAS_URI")
client = MongoClient(url)
client.admin.command("ping")
log.info("MongoDB connection successful")

db = client[os.getenv("MONGO_DB_NAME")]
collection = db[os.getenv("MONGO_COLLECTION")]
collection.create_index(
    [("city", 1), ("timestamp", 1)],
    unique=True
)


cities = json.loads(os.getenv("WEATHER_CITIES"))

def fetch_city_weather(city):

    params = {
    "latitude": city['latitude'],
    "longitude": city['longitude'],
    "current" : "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation"
}
    endpoint = os.getenv("OPEN_METEO_BASE_URL")
    resp = requests.get(endpoint, params=params,timeout=10)
    if resp.status_code == 200:

        data = resp.json()
        log.info(f"Successfully fetched weather data for {city['name']}")
        return data
    else:
        logging.warning(f"Failed to fetch weather data for {city['name']}")

def parse_and_validate(fv,city):
    current = fv.get("current",{})

    event = WeatherEvent(
        city=city['name'],
        latitude=city['latitude'],
        longitude=city['longitude'],
        temperature_2m=current.get("temperature_2m"),
        relative_humidity_2m=current.get("relative_humidity_2m"),
        wind_speed_10m=current.get("wind_speed_10m"),
        precipitation=current.get("precipitation",0.0),
        timestamp=current.get("time")
    )
        
    if event:
        log.info("Weather data validated successfully")
        return event
    else:
        #fh.write(f"Validation failed for data: {fv}\n")
        log.warning(f"Validation failed for data: {fv}")
        dead_letter.error(f"Validation failed for data: {fv}")
        return None

def insert_to_mongo(data):
    try:
        print(data.city, data.timestamp)
        collection.update_one(
            {"city": data.city, "timestamp": data.timestamp},
            {"$set": data.to_mongo_dict()},
            upsert=True
        )
        log.info("Data inserted successfully into MongoDB")
    except DuplicateKeyError:
        log.warning(f"Duplicate entry for event_id: {data.event_id}")
        dead_letter.error(f"Duplicate entry for event_id: {data.event_id}")
    except Exception as e:
        dead_letter.error(f"Error occurred while inserting data into MongoDB: {e}")



def run_producer():

    for city in cities:
        result = fetch_city_weather(city)
        if result:
            resulted_data = parse_and_validate(result,city)
            #print(f"Resulted data is {type(resulted_data)}")
            if resulted_data:
                insert_to_mongo(resulted_data) 

if __name__ == "__main__":
    run_producer()                      

