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

# Check if the number of retries was provided
if [[ -z "$2" ]]; then
    retries=3
else
    retries=$2
fi

# Log the name of the file that will be used
echo "Using cluster configuration file: $1"
echo "Number of retries for 'verify ray is running' step: $retries"

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

success=false
count=0
while [[ "$count" -lt "$retries" ]]; do
    if ray exec -v "$1" "python -c 'import ray; ray.init(\"localhost:6379\")'"; then
        success=true
        break
    else
        count=$((count+1))
        echo "Verification failed. Retry attempt $count of $retries..."
        sleep 5
    fi
done

if ! $success; then
    echo "======================================"
    echo "Error: Verification failed after $retries attempts. Please check your cluster configuration and try again."
    exit 1
fi

echo "======================================"
echo "Ray verification successful."

echo "======================================"
echo "Cleaning up cluster..."
ray down -v -y "$1"

echo "======================================"
echo "Finished executing script."
