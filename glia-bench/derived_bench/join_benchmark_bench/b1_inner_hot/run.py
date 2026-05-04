"""B1: inner_smallparts_hot.

Engages R1 (hash-shuffle map fan-out CPU) and R2 (per-partition Arrow-join
heap working-set), and records output cardinality for R4 contrast.  The
plasma store is sized so spill does NOT occur — making this the clean
"no-R3" baseline against which B4 measures R3.

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
        name="b1_inner_hot",
        n_left=24_000_000,
        n_files_left=24,
        n_right=6_000_000,
        n_files_right=12,
        join_type="inner",
        num_partitions=50,
        object_store_memory_bytes=12 * 1024**3,  # 12 GB → no spill
        n_reps=3,
        warmup=1,
    )


if __name__ == "__main__":
    main()
