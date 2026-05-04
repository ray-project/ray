"""B3: inner_manyparts_aggcap.

Same data as B1 but `num_partitions=200` (4× upstream's 50).  Each
aggregator handles ~8 partitions sequentially, so per-partition Arrow
join is ~4× smaller (R2 per-call working set drops); the shuffle map
fan-out is ~4× more output blocks (R1 amplified).  Aggregator count is
pinned at 24 to make the partitions-per-aggregator scaling visible.
"""

from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

from _common import build_and_run  # noqa: E402


def main() -> None:
    build_and_run(
        name="b3_inner_manyparts",
        n_left=24_000_000,
        n_files_left=24,
        n_right=6_000_000,
        n_files_right=12,
        join_type="inner",
        num_partitions=200,
        object_store_memory_bytes=12 * 1024**3,
        max_hash_shuffle_aggregators=24,
        n_reps=3,
        warmup=1,
    )


if __name__ == "__main__":
    main()
