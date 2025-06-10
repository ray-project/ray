#!/bin/bash

if [ $# -lt 1 ]; then
    echo "Usage: $0 [--num-runs N] -- command args..."
    exit 1
fi

NUM_RUNS=5
ARGS=()

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --num-runs)
            NUM_RUNS="$2"
            shift 2
            ;;
        --)
            shift
            ARGS+=("$@")
            break
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--num-runs N] -- command args..."
            exit 1
            ;;
    esac
done

if [ ${#ARGS[@]} -eq 0 ]; then
    echo "No command provided"
    echo "Usage: $0 [--num-runs N] -- command args..."
    exit 1
fi

echo "Running command: ${ARGS[*]}"
echo "Number of runs: $NUM_RUNS"

TOTAL_TIME=0

for i in $(seq 1 $NUM_RUNS); do
    echo "Run $i/$NUM_RUNS"

    # Time command execution and capture output
    START_TIME=$(date +%s.%N)
    "${ARGS[@]}" > "run_${i}.log" 2>&1
    EXIT_CODE=$?
    END_TIME=$(date +%s.%N)

    # Calculate duration
    DURATION=$(echo "$END_TIME - $START_TIME" | bc)
    TOTAL_TIME=$(echo "$TOTAL_TIME + $DURATION" | bc)

    echo "Duration: ${DURATION}s"
    echo "Exit Code: $EXIT_CODE"
    echo "Log: run_${i}.log"
    echo "---"
done

# Print summary
AVG_TIME=$(echo "$TOTAL_TIME / $NUM_RUNS" | bc -l)
printf "Total time: %.3fs\n" "$TOTAL_TIME"
printf "Average time per run: %.3fs\n" "$AVG_TIME"
