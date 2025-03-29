#!/bin/bash

while true; do
  ts=$(date +%Y%m%d-%H%M%S)

  output_file="/artifact-mount/ps-memories/mem-$ts.txt"

  {
    echo "Timestamp: $ts"
    echo ""
    echo "=== Memory Summary (free -h) ==="
    free -h
    echo ""
    echo "=== Process Memory Usage (ps aux --sort=-%mem) ==="
    ps aux --sort=-%mem
  } > "$output_file"

  sleep 60
done
