from minio import Minio
from minio.error import S3Error
import io
import time
import json
import random
from io import BytesIO
import pandas as pd


def list_files(minio_client, staging_bucket):
    count = 0
    try:
        objects = minio_client.list_objects(staging_bucket, recursive=True)

        for obj in objects:
            count += 1

    except S3Error as e:
        print(f"Error: {e}")
    finally:
        return count


def create_bucket(minio_client, staging_bucket):
    try:
        if not minio_client.bucket_exists(staging_bucket):
            minio_client.make_bucket(staging_bucket)
            print(f"Bucket '{staging_bucket}' created successfully.")

    except S3Error as e:
        print(f"Error: {e}")


def get_and_delete_first_file(minio_client, staging_bucket):
    try:
        # Get the first object in the bucket
        objects = minio_client.list_objects(staging_bucket)
        first_object = next(objects)
        object_name = first_object.object_name

        # Download data from the object
        data = minio_client.get_object(staging_bucket, object_name).read()

        # Delete the object from the bucket
        minio_client.remove_object(staging_bucket, object_name)

        return data

    except StopIteration:
        # No objects in the bucket
        return None

    except S3Error as e:
        print(f"Error: {e}")
        return None


def bytes_to_dict(byte_data):
    try:
        # Decode the bytes data using UTF-8 and load it as a JSON object
        decoded_data = byte_data.decode("utf-8")
        dictionary = json.loads(decoded_data)
        return dictionary
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None


if __name__ == "__main__":
    minio_endpoint = "minio:9000"
    minio_access_key = "minioaccesskey"
    minio_secret_key = "miniosecretkey"
    secure = False

    minio_client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=secure,
    )

    staging_bucket = "video-data-bucket"
    querying_bucket = "query-bucket"
    create_bucket(minio_client, staging_bucket)
    create_bucket(minio_client, querying_bucket)
    try:
        minio_client.stat_object(querying_bucket, "db.parquet")
        print(f"The file  already exists in the bucket .")
    except:
        data = []
        minio_client.fput_object(querying_bucket, "db.parquet", "./db.parquet")
        print("file crated")

    while True:
        while list_files(minio_client, staging_bucket) == 0:
            print("waiting for files to upload...")
            time.sleep(5)

        file_data = get_and_delete_first_file(minio_client, staging_bucket)

        if file_data is not None:
            for post_data in bytes_to_dict(file_data):
                post_data["score"] = random.randint(30, 100)
                parquet_data = minio_client.get_object(querying_bucket, "db.parquet")
                parquet_bytes = BytesIO(parquet_data.read())

                parquet_table = pd.read_parquet(parquet_bytes)
                temp_df = pd.DataFrame([post_data])
                parquet_table = pd.concat([parquet_table, temp_df], ignore_index=True)
                parquet_table.to_parquet("temp.parquet")
                minio_client.fput_object(
                    querying_bucket, "db.parquet", "./temp.parquet"
                )
