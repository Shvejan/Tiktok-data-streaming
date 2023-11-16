from kafka import KafkaProducer
import json


producer = KafkaProducer(
    bootstrap_servers=["broker:29092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

data = {"key": "value"}  # Your data here

for i in range(3):
    producer.send("video_data", data)
producer.flush()
