from confluent_kafka.admin import AdminClient, NewTopic
import os
from dotenv import load_dotenv


load_dotenv()

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
admin = AdminClient({"bootstrap.servers": bootstrap_servers})

topics = [NewTopic("weather_events", num_partitions=1, replication_factor=1)]

fs = admin.create_topics(topics)

for topic, f in fs.items():
    try:
        f.result()
        print(f"Topic {topic} created successfully")
    except Exception as e:
        print(f"Failed to create topic {topic}: {e}")    