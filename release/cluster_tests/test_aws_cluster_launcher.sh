#!/bin/bash
# Test Ray Data bulk ingestion performance as the size of input files change.

set -x -e

ray down -y ./aws_cluster.yaml
ray up -y ./aws_cluster.yaml
ray exec ./aws_cluster.yaml 'python -c "import ray; ray.init()"'
ray down -y ./aws_cluster.yaml
