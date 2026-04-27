#!/bin/bash
# Daft shuffle benchmark matrix.
# Runs data-size x partition-count sweep with 20s cooldown between runs.
# Each run is a separate process because Daft requires ray.shutdown().

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$SCRIPT_DIR/benchmark_ooc_daft_shuffle.py"
RESULTS_DIR="$SCRIPT_DIR/daft_matrix_results"
mkdir -p "$RESULTS_DIR"

DATA_SIZES=(10 50 100 170)
PARTITIONS=(100 200 500 1000)

echo "=== Daft Shuffle Benchmark Matrix ==="
echo "Data sizes: ${DATA_SIZES[*]} GB"
echo "Partitions: ${PARTITIONS[*]}"
echo "Results dir: $RESULTS_DIR"
echo ""

FIRST=1
for gb in "${DATA_SIZES[@]}"; do
    for p in "${PARTITIONS[@]}"; do
        if [ "$FIRST" -eq 1 ]; then
            FIRST=0
        else
            echo "  cooldown 20s ..."
            sleep 20
        fi

        OUTPUT="$RESULTS_DIR/daft_${gb}gb_${p}p.json"
        echo "--- ${gb} GB, ${p} partitions ---"
        python "$SCRIPT" --data-size-gb "$gb" --num-partitions "$p" --output "$OUTPUT" || echo "  FAILED: ${gb}GB/${p}p"
        echo ""
    done
done

# Print summary
echo ""
echo "=== Summary ==="
printf "%-10s %-12s %10s\n" "Data(GB)" "Partitions" "Time(s)"
echo "--------------------------------------"
for gb in "${DATA_SIZES[@]}"; do
    for p in "${PARTITIONS[@]}"; do
        FILE="$RESULTS_DIR/daft_${gb}gb_${p}p.json"
        if [ -f "$FILE" ]; then
            T=$(python -c "import json; d=json.load(open('$FILE')); print(f\"{d.get('elapsed_s', 'N/A')}\")" 2>/dev/null || echo "ERR")
            printf "%-10s %-12s %10s\n" "$gb" "$p" "$T"
        else
            printf "%-10s %-12s %10s\n" "$gb" "$p" "MISSING"
        fi
    done
done
