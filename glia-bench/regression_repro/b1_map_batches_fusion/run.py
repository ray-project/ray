"""B1: Read→MapBatches operator fusion.

Investigation outcome (see SCOPE.md):

- Glia (2.55.0 base) does not support batch_size="auto" — it raises a
  TypeError before any Dataset executes. Master added "auto" support
  after 2.55.0.
- Master fuses ReadParquet→MapBatches into ONE TaskPoolMapOperator
  ONLY when batch_size="auto" (or unset) and the Read producer's
  additional_split_factor == 1 (line 227 of operator_fusion.py).
- The friend's release test (`map_benchmark.py`) on his glia branch
  defaulted to batch_size=10000; on upstream master it now defaults
  to batch_size="auto". So 91513 used 10000 and 91797 used "auto",
  triggering different op-graphs.

This bench reproduces the fusion gap by running 3 configurations:

| cell                       | tree    | batch_size | fusion expected |
| glia_b10k                  | m6_fixed| 10_000     | NO              |
| master_b10k                | master  | 10_000     | NO              |
| master_auto                | master  | "auto"     | YES             |

Wall delta master_b10k → master_auto isolates the fusion benefit.
Wall delta glia_b10k → master_auto is the full "regression" the
friend's comparison.md attributes to map_batches_*.

Calibration target: producer files that meet split_factor=1 on the
default min_parallelism=200. We use ROW_GROUP rows per row group with
multiple row groups per file so natural block count >= 200.
"""

from __future__ import annotations

import argparse
import functools
import json
import os
import sys
import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common import run_bench, shutdown_ray  # noqa: E402

import ray  # noqa: E402

# Calibrated to satisfy split_factor=1 on default min_parallelism=200.
# 100 files × 5 row groups = 500 natural blocks (>>200) so split=1.
# Per file is ~96MB; total ~9.6GB parquet. Reads are real work, not toy.
N_FILES = 100
N_ROWS_PER_FILE = 1_000_000
ROW_GROUP_SIZE = 200_000
N_COLUMNS = 12
SLEEP_MS = 5
N_REPS = 3


def _make_parquet_files(out_dir: str) -> None:
    if os.path.isdir(out_dir) and len(os.listdir(out_dir)) >= N_FILES:
        return
    os.makedirs(out_dir, exist_ok=True)
    rng = np.random.default_rng(0)
    for i in range(N_FILES):
        cols = {
            f"column{c:02d}": rng.integers(0, 1_000_000, N_ROWS_PER_FILE).astype(np.int64)
            for c in range(N_COLUMNS)
        }
        table = pa.table(cols)
        pq.write_table(
            table,
            f"{out_dir}/part_{i:03d}.parquet",
            row_group_size=ROW_GROUP_SIZE,
            compression=None,
        )


def _increment_batch(batch, sleep_ms: int = 0):
    if sleep_ms > 0:
        time.sleep(sleep_ms / 1000.0)
    column00_incremented = pc.add(batch["column00"], 1)
    return batch.set_column(0, "column00", column00_incremented)


def parse_batch_size(s: str):
    if s == "auto":
        return "auto"
    return int(s)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--batch-size",
        type=parse_batch_size,
        default=10_000,
        help="batch_size for map_batches: int or the string 'auto'",
    )
    parser.add_argument("--cell", default="default", help="label for output JSON")
    args = parser.parse_args()

    parquet_dir = "/tmp/regression_repro_b1_parquet_v2"
    _make_parquet_files(parquet_dir)

    bs = args.batch_size

    def workload():
        # override_num_blocks=N_FILES forces additional_split_factor=1
        # by aligning desired parallelism with natural file count, so the
        # operator-fusion gate (line 227) doesn't reject Read+Map.
        ds = ray.data.read_parquet(parquet_dir, override_num_blocks=N_FILES)
        ds = ds.map_batches(
            functools.partial(_increment_batch, sleep_ms=SLEEP_MS),
            batch_format="pyarrow",
            batch_size=bs,
        )
        ds = ds.map_batches(
            functools.partial(_increment_batch, sleep_ms=SLEEP_MS),
            batch_format="pyarrow",
            batch_size=bs,
        )
        ds = ds.map_batches(
            lambda b: {"num_rows": [len(b["column00"])]},
            batch_format="pyarrow",
        )
        for _ in ds.iter_internal_ref_bundles():
            pass
        return ds

    init_kwargs = dict(
        num_cpus=24,
        log_to_driver=False,
        logging_level="WARNING",
        include_dashboard=False,
        object_store_memory=8 * 1024**3,
    )
    run_bench(
        name=f"b1_map_batches_fusion[{args.cell}]",
        init_kwargs=init_kwargs,
        workload=workload,
        n_reps=N_REPS,
        warmup=True,
    )
    shutdown_ray()


if __name__ == "__main__":
    main()
