#!/bin/bash

# Print and log the memory usage of dashboard.py every 10 seconds.
# Usage: log_memory.sh 
# Example: log_memory.sh 
# The output will be written to memory.log.

# Get pid of dashboard.py
pid=$(ps aux | grep dashboard.py | grep -v grep | awk '{print $2}')
echo "PID: $pid"

echo "Logging memory usage of process $pid every 10 seconds."
echo "Press Ctrl+C to stop."
echo "Memory (MB) every 10 seconds" > memory.log
while true; do
  mem=$(ps -o rss= -p $pid | awk '{print $1/1024}') 
  echo "$mem" >> memory.log
  echo $mem
  sleep 10
  
done