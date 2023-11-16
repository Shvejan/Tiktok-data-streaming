#!/bin/sh
# wait-for.sh

set -e

until nc -z "$1" && nc -z "$2"; do
  echo "Waiting for services..."
  sleep 1
done

echo "Services are up - executing command"
exec "$@"
