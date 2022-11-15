#!/bin/bash
# Test basics of Ray Cluster Launcher.

set -e

echo "===== clean up previously running cluster ====="
ray down -y ./aws_cluster.yaml

sleep 5
echo "===== start cluster ====="
ray up -y ./aws_cluster.yaml

sleep 5
echo "===== verify ray is running ====="
ray exec ./aws_cluster.yaml 'python -c "import ray; ray.init()"'

sleep 5
echo "===== clean up ====="
ray down -y ./aws_cluster.yaml
