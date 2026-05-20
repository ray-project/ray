"""B1: agg_sort_lowcard.

Engages R1 (sort-shuffle all-to-all bandwidth saturation).  Aggregate path
(`mean("column05")`) under sort-shuffle-pull-based, with a low-cardinality
key (column08, column13, column14 → 84 group combinations).

The whole 60M-row dataset moves through the shuffle; with default 9.3 GiB
plasma the pre-aggregation map output spills naturally, but the shuffle
compute (sample + shuffle map + shuffle reduce) is the dominant cost.

Run:
    /opt/venv/bin/python run.py

Validation lever (Phase 3 record): swap consume to map_groups (B3) keeps
the same shuffle but adds R3+R4 → wall +17%, spill +3×.  Removing the
shuffle (read-only baseline) drops wall ≈10×.
"""

from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

from _common import build_and_run  # noqa: E402


def main() -> None:
    build_and_run(
        name="b1_agg_sort_lowcard",
        rows=60_000_000,
        files=50,
        consume="aggregate",
        shuffle_strategy="sort_shuffle_pull_based",
        group_by=["column08", "column13", "column14"],
        override_num_blocks=50,
        n_reps=3,
        warmup=1,
    )


if __name__ == "__main__":
    main()
