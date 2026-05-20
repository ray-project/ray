"""B5: agg_sort_spill.

Engages R3 (object-store spill under cap) at intensity.  Same logical
workload as B1 (60M-row aggregate sort-shuffle, low-card key) but with
the plasma object store explicitly capped at 2 GiB so the all-to-all
shuffle's working set deterministically exceeds the cap and Ray spills
~9.4 GiB to local disk during the shuffle.

Sets `RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1` because the cap (2 GB) fits
in /dev/shm but lifting it would not — keeps the env consistent with B5's
validity control (`B5_validity_lift_cap` runs the same workload with a
32 GB cap).

Phase-3 evidence: with the cap lifted to 32 GiB, spill drops to 0 and
wall drops from 91.7 s → 49.1 s.  Both signals respond to the trigger;
the cap is what engages R3.

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
        name="b5_agg_sort_spill",
        rows=60_000_000,
        files=50,
        consume="aggregate",
        shuffle_strategy="sort_shuffle_pull_based",
        group_by=["column08", "column13", "column14"],
        override_num_blocks=50,
        object_store_memory_bytes=2_000_000_000,
        n_reps=3,
        warmup=1,
    )


if __name__ == "__main__":
    main()
