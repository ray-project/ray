#!/bin/bash

NUM_PROCESSES=400
for i in $(seq 1 $NUM_PROCESSES); do
    python3 /home/ubuntu/ray/experimental/start_ray_job.py &
done

sleep 3600
