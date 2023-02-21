#!/bin/bash
# Test basics of Ray Cluster Launcher.

set -e

# Check if a command line argument was provided
if [[ -z "$1" ]]; then
    echo "Please provide a path to the cluster configuration file as a command line argument."
    exit 1
fi

# Check if the file exists and is readable
if [[ ! -r "$1" ]]; then
    echo "Error: Cannot read cluster configuration file: $1"
    exit 1
fi

# Log the name of the file that will be used
echo "Using cluster configuration file: $1"

echo "===== clean up previously running cluster ====="
ray down -y "$1"

sleep 5
echo "===== start cluster ====="
ray up -y "$1"

sleep 5
echo "===== verify ray is running ====="
ray exec "$1" 'python -c "import ray; ray.init('localhost:6379')"'

sleep 5
echo "===== clean up ====="
ray down -y "$1"
