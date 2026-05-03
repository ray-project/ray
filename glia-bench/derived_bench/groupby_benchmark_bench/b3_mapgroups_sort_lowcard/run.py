"""B3: mapgroups_sort_lowcard.

Engages R4 (per-group heap blow-up under map_groups) and R3 (object-store
spill) and R1 (sort-shuffle bandwidth).  Map_groups path on the same 60M
sort-shuffle workload as B1 with the same low-cardinality key, but the
consume function is `map_groups(normalize_table)` so each group must be
materialised in a single batch on a worker — peak per-task heap scales
with `rows / num_groups`, which is ~700 K rows here.

Phase-3 evidence: spill goes from 8.7 GiB (B1, aggregate fuses partial
reductions into shuffle map) to 25.5 GiB (B3, whole groups in memory),
wall +17%.  The spill delta is the R4 signal; the wall delta is the R3
manifestation of the larger working set.

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
        name="b3_mapgroups_sort_lowcard",
        rows=60_000_000,
        files=50,
        consume="map_groups",
        shuffle_strategy="sort_shuffle_pull_based",
        group_by=["column08", "column13", "column14"],
        override_num_blocks=50,
        n_reps=3,
        warmup=1,
    )


if __name__ == "__main__":
    main()
