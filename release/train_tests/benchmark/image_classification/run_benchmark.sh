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

TOTAL_NANO=0

for i in $(seq 1 $NUM_RUNS); do
    echo "Run $i/$NUM_RUNS"

    START_TIME_NS=$(date +%s%N)
    "${ARGS[@]}" > "run_${i}.log" 2>&1
    EXIT_CODE=$?
    END_TIME_NS=$(date +%s%N)

    DURATION_NS=$((END_TIME_NS - START_TIME_NS))
    TOTAL_NANO=$((TOTAL_NANO + DURATION_NS))

    DURATION_SEC=$((DURATION_NS / 1000000000))
    DURATION_MS=$(((DURATION_NS / 1000000) % 1000))
    echo "Duration: ${DURATION_SEC}.${DURATION_MS}s"
    echo "Exit Code: $EXIT_CODE"
    echo "Log: run_${i}.log"
    echo "---"
done

AVG_NANO=$((TOTAL_NANO / NUM_RUNS))
AVG_SEC=$((AVG_NANO / 1000000000))
AVG_MS=$(((AVG_NANO / 1000000) % 1000))
TOTAL_SEC=$((TOTAL_NANO / 1000000000))
TOTAL_MS=$(((TOTAL_NANO / 1000000) % 1000))

printf "Total time: %d.%03ds\n" "$TOTAL_SEC" "$TOTAL_MS"
printf "Average time per run: %d.%03ds\n" "$AVG_SEC" "$AVG_MS"
