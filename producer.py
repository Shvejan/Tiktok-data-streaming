from kafka import KafkaProducer
import json
import time


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

data = {"key": "value"}  # Your data here

while True:
    time.sleep(1)
    producer.send("video_data", data)
    producer.flush()
