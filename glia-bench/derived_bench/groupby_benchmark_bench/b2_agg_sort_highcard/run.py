"""B2: agg_sort_highcard.

Engages R5 (boundary-sampling cost amplified at high cardinality) and R1
(sort-shuffle bandwidth).  Aggregate path on a 6M-row dataset with a
high-cardinality key (column02 has up to ~2M distinct values; combined
with column14 the resulting partition boundaries are non-trivial to
sample).

Phase-3 cardinality A/B (`B2_validity_lowcard`) showed swapping the key
to a low-cardinality one cuts wall time ~19× on the same dataset, which
is the regime signal: time is dominated by sample + shuffle work that
scales with cardinality.

Run:
    /opt/venv/bin/python run.py

The benchmark exposes the `Sort sample` cost via the optional
`SortTaskSpec.sample_boundaries` patch (signature-forwarding) so an
optimisation targeting the sample stage can read it from the per-rep
extras.
"""

from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

from _common import build_and_run  # noqa: E402


def main() -> None:
    build_and_run(
        name="b2_agg_sort_highcard",
        rows=6_000_000,
        files=24,
        consume="aggregate",
        shuffle_strategy="sort_shuffle_pull_based",
        group_by=["column02", "column14"],
        override_num_blocks=24,
        n_reps=3,
        warmup=1,
        patch_sample=True,
    )


if __name__ == "__main__":
    main()
