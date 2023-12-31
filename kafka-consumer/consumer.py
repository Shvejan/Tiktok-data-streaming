from kafka import KafkaConsumer
from minio import Minio
import json
import os
from minio.error import S3Error


def create_bucket(minio_client, staging_bucket):
    try:
        if not minio_client.bucket_exists(staging_bucket):
            minio_client.make_bucket(staging_bucket)
            print(f"Bucket '{staging_bucket}' created successfully.")

    except S3Error as e:
        print(f"Error: {e}")


# Kafka Consumer Configuration
kafka_consumer = KafkaConsumer(
    "video_data",  # Kafka topic
    bootstrap_servers=["broker:29092"],  # Kafka broker address
    auto_offset_reset="earliest",
    group_id="my-group",  # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

# MinIO Configuration
minio_client = Minio(
    "minio:9000",  # MinIO endpoint
    access_key="minioaccesskey",  # MinIO access key
    secret_key="miniosecretkey",  # MinIO secret key
    secure=False,  # Set to True for HTTPS
)
bucket_name = "video-data-bucket"
create_bucket(minio_client, bucket_name)

output_directory = "./output"
os.makedirs(output_directory, exist_ok=True)
json_file_path = os.path.join(output_directory, "output.json")

data = []
count = 1
print("waiting for messages....")
for message in kafka_consumer:
    data.append(message.value)
    if len(data) >= 10:  # grouping the data in batches of 10
        print("output " + str(count) + " uploaded ")
        with open(json_file_path, "w") as file:
            json.dump(data, file)

        minio_client.fput_object(  # uploading json data into minio s3 bucket
            bucket_name, "output" + str(count) + ".json", json_file_path
        )
        count += 1
        os.remove(json_file_path)
        data = []
        print("waiting for messages...")


kafka_consumer.close()
