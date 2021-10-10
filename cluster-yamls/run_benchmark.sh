#!/bin/bash

set -x

num_nodes=$1
system=$2
object_store_memory_gb=$3

if [ $# -ne 3 ]; then
    echo "Usage: run_benchmark.sh <number of nodes total> <system=memory|ray> <object store size (GB)>"
    exit
fi


activate_env=""
if [ $system == "ray" ]; then
    activate_env="source activate ray-master && "
fi

# Warmup.
object_size_mb=100
num_objects=100
command="python ray/python/ray/tests/test_benchmark.py --system $system --num-nodes $num_nodes --object-size-mb $object_size_mb --num-objects-per-node $num_objects --output-filename output.csv --object-store-memory-mb "$object_store_memory_gb"_000"
$command

for object_size_mb in 100 10 1; do
    num_objects=$(( 9000 / $object_size_mb ))
    command="python ray/python/ray/tests/test_benchmark.py --system $system --num-nodes $num_nodes --object-size-mb $object_size_mb --num-objects-per-node $num_objects --output-filename output.csv --object-store-memory-mb "$object_store_memory_gb"_000"
    $command
done

object_size_mb=100
for num_objects in 500 1000 2000; do
    for num_args in 1 2; do
        num_objects=$(( num_objects / num_args ))
        command="python ray/python/ray/tests/test_benchmark.py --system $system --num-nodes $num_nodes --object-size-mb $object_size_mb --num-objects-per-node $num_objects --output-filename output.csv --object-store-memory-mb "$object_store_memory_gb"_000 --num-pipeline-args $num_args"
        $command
    done
done
