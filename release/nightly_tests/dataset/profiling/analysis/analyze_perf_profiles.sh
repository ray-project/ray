#!/bin/bash
# ABOUTME: Converts all perf collapsed stack files to speedscope JSON and generates a thread summary.
# ABOUTME: Run from a directory containing perf_*_collapsed.txt files.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

outfile="perf_thread_summary.txt"
: > "$outfile"

for collapsed in perf_*_collapsed.txt; do
    [ -f "$collapsed" ] || continue
    base="${collapsed%_collapsed.txt}"
    speedscope="${base}.speedscope.json"

    echo "Converting $collapsed..."
    "$SCRIPT_DIR/collapsed_to_speedscope.py" "$collapsed" -o "$speedscope"
    echo

    {
        echo "=== $base ==="
        "$SCRIPT_DIR/analyze_pyspy_profile.py" "$speedscope" --list-threads
        echo
    } >> "$outfile"
done

echo "Thread summary written to $outfile"
