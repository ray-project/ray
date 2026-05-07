"""B4: read-side regression — wide schema parquet read.

Upstream wide_schema_pipeline_tensors:
- glia 91513:    42s wall, just `Input -> ReadParquet`
- master 91797:  19s wall, same op graph

Same op graph + same DataContext (modulo batch_to_block_arrow_format),
so the regression is in the read path itself — block-size profile, chunk
combining, or schema-yielding.

Suspect commits on master since 2.55.0:
- #62579: Set chunk-combining threshold to 1 for batcher (small buffers
  caused regression when not pre-combined)
- #62720: Yield only first schema in _map_task (~25% deserialization
  reduction in author's benchmark)

Bench: write a wide schema (200 columns), with values that produce
many small chunks per row group (RowGroup containing several batches
with small batch sizes per Arrow Table). Then just read it.

Validity: glia and master should both have op graph =
"Input -> ReadParquet". Wall delta should reflect the chunk-combining /
schema-yield improvements.
"""

from __future__ import annotations

import os
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common import run_bench, shutdown_ray  # noqa: E402

import ray  # noqa: E402


N_FILES = 24                # = num_cpus → one task per file
N_ROWS_PER_FILE = 500_000   # bigger per file so read isn't trivial
ROW_GROUP_SIZE = 5_000      # smallish row groups → many small chunks per row group
N_COLUMNS = 200             # wide-schema regime
N_REPS = 3


def _make_wide_schema_files(out_dir: str) -> None:
    if os.path.isdir(out_dir) and any(os.scandir(out_dir)):
        return
    os.makedirs(out_dir, exist_ok=True)
    rng = np.random.default_rng(0)

    # Pre-build a fixed string pool so file-gen doesn't pay
    # python-list-of-strings construction cost per column.
    str_pool_arr = pa.array([f"v{p:015d}" for p in range(2048)])

    for i in range(N_FILES):
        cols = {}
        for c in range(N_COLUMNS):
            kind = c % 3
            if kind == 0:
                cols[f"int_{c:03d}"] = pa.array(
                    rng.integers(0, 1_000_000, N_ROWS_PER_FILE, dtype=np.int64)
                )
            elif kind == 1:
                cols[f"float_{c:03d}"] = pa.array(
                    rng.random(N_ROWS_PER_FILE).astype(np.float64)
                )
            else:
                # Map to pre-built string pool — vectorized, no Python loop.
                idx = rng.integers(0, len(str_pool_arr), N_ROWS_PER_FILE)
                cols[f"str_{c:03d}"] = str_pool_arr.take(pa.array(idx))
        table = pa.table(cols)
        pq.write_table(
            table,
            f"{out_dir}/part_{i:03d}.parquet",
            row_group_size=ROW_GROUP_SIZE,
            compression=None,
        )


def main() -> None:
    parquet_dir = "/tmp/regression_repro_b4_wide_schema"
    _make_wide_schema_files(parquet_dir)

    def workload():
        ds = ray.data.read_parquet(parquet_dir)
        for _ in ds.iter_internal_ref_bundles():
            pass
        return ds

    init_kwargs = dict(
        num_cpus=24,
        log_to_driver=False,
        logging_level="DEBUG",
        include_dashboard=False,
        object_store_memory=8 * 1024**3,
    )
    run_bench(
        name="b4_wide_schema_read",
        init_kwargs=init_kwargs,
        workload=workload,
        n_reps=N_REPS,
        warmup=True,
    )
    shutdown_ray()


if __name__ == "__main__":
    main()
