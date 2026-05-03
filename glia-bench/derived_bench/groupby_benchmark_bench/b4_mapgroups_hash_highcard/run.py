"""B4: mapgroups_hash_highcard.

Engages R2 (hash-aggregator actor pool sizing / steady-state pressure).
Hash-shuffle strategy with 8 max aggregators × 16 reduce partitions; the
high-cardinality key (column02 column14) routes evenly across the pool.
Map_groups consume forces a per-group-per-row UDF call on the highcard
output, so the regime cost is dominated by the hash-shuffle map → reduce
→ map_groups pipeline, not by per-group memory blow-up (R4 is absent
because every group is ~1 row at this cardinality).

Phase-3 strategy A/B (`B4_validity_sort`): swapping hash → sort on the
same workload increases wall from ~83 s to ~120 s.  The hash-aggregator
pool is on the critical path.

Run:
    /opt/venv/bin/python run.py
"""

from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

from _common import build_and_run  # noqa: E402


def main() -> None:
    build_and_run(
        name="b4_mapgroups_hash_highcard",
        rows=3_000_000,
        files=16,
        consume="map_groups",
        shuffle_strategy="hash_shuffle",
        group_by=["column02", "column14"],
        override_num_blocks=16,
        max_hash_aggregators=8,
        hash_shuffle_parallelism=16,
        n_reps=3,
        warmup=1,
    )


if __name__ == "__main__":
    main()
