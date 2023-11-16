#!/bin/sh
# wait-for.sh

set -e

# Function to check if a service is up by trying to connect to its port
wait_for_service() {
  SERVICE="$1"
  PORT="$2"
  echo "Waiting for $SERVICE to be ready..."
  until nc -z "$SERVICE" "$PORT"; do
    echo "Still waiting for $SERVICE..."
    sleep 1
  done
  echo "$SERVICE is up and running."
}

# Wait for MinIO and Kafka broker
wait_for_service "minio" 9000
wait_for_service "broker" 29092


echo "All required services are up - executing command"
# Execute the passed command
