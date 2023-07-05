#!/bin/bash

# Set the default backend
backend="cudagraphs"
# Set the default mode
mode="default"
# Set the default CPU flag
cpu_flag=""

# Check if the --backend, --mode, and --cpu options are provided
while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
        --backend)
        backend="$2"
        shift
        shift
        ;;
        --mode)
        mode="$2"
        shift
        shift
        ;;
        --cpu)
        cpu_flag="--cpu"
        shift
        ;;
        *)
        shift
        ;;
    esac
done

# Define the batch sizes
batch_sizes=(1 4 16)

# Loop through the batch sizes
for bs in "${batch_sizes[@]}"
do
    # Call the Python script with the batch size argument
    echo "Running for batch size $bs"
    python rllib/benchmarks/torch_compile/run_inference_bm.py -bs "$bs" --backend "$backend" --mode "$mode" $cpu_flag
done
