#!/bin/bash
# Start worker process
pkill -f worker.py 2>/dev/null
sleep 1
export PYTHONUNBUFFERED=1
nohup python3 /home/ubuntu/worker.py --gcs-address "$1" > /tmp/worker.log 2>&1 &
sleep 4
cat /tmp/worker.log
