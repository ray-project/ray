"""B4: inner_smallplasma_spill.

Same workload as B1 but with `object_store_memory=1.5 GB`, far below the
shuffle working set.  `RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1` lets Ray
host the small plasma on /tmp.  R3 (object-store spill because the
shuffled-shard working set exceeds plasma) engages strongly.

The wall-time and `spilled_bytes_total` deltas vs. B1 isolate R3.
"""

from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

from _common import build_and_run  # noqa: E402


def main() -> None:
    build_and_run(
        name="b4_inner_spill",
        n_left=24_000_000,
        n_files_left=24,
        n_right=6_000_000,
        n_files_right=12,
        join_type="inner",
        num_partitions=50,
        object_store_memory_bytes=int(4.5 * 1024**3),  # 4.5 GB → spill engaged, workload completes
        n_reps=3,
        warmup=1,
    )


if __name__ == "__main__":
    main()
