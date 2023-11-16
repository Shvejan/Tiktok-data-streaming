from kafka import KafkaConsumer
import json

# To consume messages
consumer = KafkaConsumer(
    "video_data",  # Replace 'your_topic' with your Kafka topic
    bootstrap_servers=["localhost:9092"],  # Change as per your Kafka broker address
    auto_offset_reset="earliest",
    group_id="my-group",  # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

for message in consumer:
    print(f"Received message: {message.value}")
    print("\n")
    print("\n")
    print("\n")

consumer.close()
