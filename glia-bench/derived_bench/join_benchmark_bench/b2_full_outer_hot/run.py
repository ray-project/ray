"""B2: full_outer_smallparts_hot.

Same data and parameters as B1 but `join_type=full_outer`.  Engages R1,
R2, and the heaviest R4 cell (output ≈ left ∪ right with NULLs filled).
Wall delta vs. B1 is the join-type cost when shuffle is held constant.
"""

from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

from _common import build_and_run  # noqa: E402


def main() -> None:
    build_and_run(
        name="b2_full_outer_hot",
        n_left=24_000_000,
        n_files_left=24,
        n_right=6_000_000,
        n_files_right=12,
        join_type="full_outer",
        num_partitions=50,
        object_store_memory_bytes=12 * 1024**3,
        n_reps=3,
        warmup=1,
    )


if __name__ == "__main__":
    main()
