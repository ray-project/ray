#!/bin/bash
# Test basics of Ray Cluster Launcher.

set -e

# Check if a command line argument was provided
if [[ -z "$1" ]]; then
    echo "Error: Please provide a path to the cluster configuration file as a command line argument."
    exit 1
fi

# Check if the file exists and is readable
if [[ ! -r "$1" ]]; then
    echo "Error: Cannot read cluster configuration file: $1"
    exit 1
fi

# Log the name of the file that will be used
echo "Using cluster configuration file: $1"

# Download the ssh key
echo "======================================"
echo "Downloading ssh key..."
python download_ssh_key.py

echo "======================================"
echo "Cleaning up any previously running cluster..."
ray down -v -y "$1"

echo "======================================"
echo "Starting new cluster..."
ray up -v -y "$1"

echo "======================================"
echo "Verifying Ray is running..."
ray exec -v "$1" "python -c 'import ray; ray.init(\"localhost:6379\")'"

echo "======================================"
echo "Cleaning up cluster..."
ray down -v -y "$1"

echo "======================================"
echo "Finished executing script."
