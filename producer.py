from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],  # Change as per your Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

data = {"key": "value"}  # Your data here

producer.send("video_data", data)
producer.flush()
