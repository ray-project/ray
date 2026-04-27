#!/bin/bash
# Ray Data actorless shuffle benchmark matrix.
# 16-node cluster (m5.2xlarge, 8 CPU / 30 GB each).
# 20-second cooldown between runs.
#
# Usage:
#     bash benchmark_raydata_matrix.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$SCRIPT_DIR/benchmark_ooc_shuffle.py"
RESULTS_DIR="$SCRIPT_DIR/raydata_matrix_results"
mkdir -p "$RESULTS_DIR"

DATA_SIZES=(10 50 100 170)
PARTITIONS=(100 200 500 1000)

echo "=== Ray Data Actorless Shuffle Benchmark Matrix ==="
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

        OUTPUT="$RESULTS_DIR/ray_${gb}gb_${p}p.json"
        echo "--- ${gb} GB, ${p} partitions ---"
        python "$SCRIPT" --data-size-gb "$gb" --num-partitions "$p" --strategy actorless --output "$OUTPUT" || echo "  FAILED: ${gb}GB/${p}p"
        echo ""
    done
done

# Print summary table
echo ""
echo "=== Time (seconds) ==="
printf "%-10s" "Data(GB)"
for p in "${PARTITIONS[@]}"; do
    printf "%12s" "p=$p"
done
echo ""
printf '%0.s-' {1..58}
echo ""
for gb in "${DATA_SIZES[@]}"; do
    printf "%-10s" "$gb"
    for p in "${PARTITIONS[@]}"; do
        FILE="$RESULTS_DIR/ray_${gb}gb_${p}p.json"
        if [ -f "$FILE" ]; then
            T=$(python -c "import json; d=json.load(open('$FILE')); print(f\"{d.get('elapsed_s', 'N/A'):.1f}\")" 2>/dev/null || echo "ERR")
            printf "%12s" "${T}s"
        else
            printf "%12s" "MISSING"
        fi
    done
    echo ""
done

# Print throughput table
echo ""
echo "=== Throughput (GB/s) ==="
printf "%-10s" "Data(GB)"
for p in "${PARTITIONS[@]}"; do
    printf "%12s" "p=$p"
done
echo ""
printf '%0.s-' {1..58}
echo ""
for gb in "${DATA_SIZES[@]}"; do
    printf "%-10s" "$gb"
    for p in "${PARTITIONS[@]}"; do
        FILE="$RESULTS_DIR/ray_${gb}gb_${p}p.json"
        if [ -f "$FILE" ]; then
            TP=$(python -c "
import json
d=json.load(open('$FILE'))
t=d.get('elapsed_s',0)
g=d.get('actual_gb',$gb)
print(f'{g/t:.1f}' if t and t>0 else 'N/A')
" 2>/dev/null || echo "ERR")
            printf "%12s" "$TP"
        else
            printf "%12s" "MISSING"
        fi
    done
    echo ""
done
