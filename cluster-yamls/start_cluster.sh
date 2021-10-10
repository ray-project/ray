#!/bin/bash

set -x


num_nodes=$1
system=$2
object_store_memory_gb=$3

if [ $# -ne 3 ]; then
    echo "Usage: start_cluster.sh <number of nodes total> <system=memory|ray> <object store size (GB)>"
    exit
fi

if [ $system != "memory" ] && [ $system != "ray" ]; then
    echo "system must be <memory|ray>"
    exit
fi

echo "Starting $system cluster with $num_nodes nodes, $object_store_memory_gb GB memory each"
template="cluster-yamls/$system.yaml.template"
cluster_yaml="cluster-yamls/$system.yaml"
echo "Using template file $template"

num_workers=$(( $num_nodes - 1 ))
sed 's/{num_workers}/'$num_workers'/' $template | sed 's/{object_store_memory}/'$object_store_memory_gb'_000_000_000/' > $cluster_yaml


activate_env=""
if [ $system == "ray" ]; then
    activate_env="source activate ray-master && "
fi

echo `date` > ~/ray-empty/date; ray up -y $cluster_yaml
ray exec $cluster_yaml "$activate_env which python && python ray/benchmarks/distributed/wait_cluster.py --num-nodes '$num_nodes'"
