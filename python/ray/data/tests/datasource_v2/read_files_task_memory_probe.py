#!/usr/bin/env python3
"""Probe ReadFiles Ray ``memory`` reservations against the live module default.

ListFiles workers run in separate processes and load
``READ_FILES_TASK_MEMORY_EPS_BYTES`` from the installed package, so mutating that
name in this driver process does **not** change worker-side task estimates. This
script only records what the scheduler merged and compares it to the formula using
``read_files_task_memory.READ_FILES_TASK_MEMORY_EPS_BYTES`` as imported here (same
as workers if you run from an editable install of this tree).

Run from the ``python/`` directory::

    cd python
    python ray/data/tests/datasource_v2/read_files_task_memory_probe.py

See ``README_read_files_task_memory.md`` for tuning the constant in source.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq

import ray
from ray.data._internal.datasource_v2.readers import read_files_task_memory as rftm
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    _parquet_max_uncompressed_row_group_bytes,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data.context import DataContext


def main() -> None:
    eps = int(rftm.READ_FILES_TASK_MEMORY_EPS_BYTES)
    captured: List[int] = []
    orig = MapOperator._merge_task_memory_into_ray_remote_args

    def recorder(self, input_bundle, ray_remote_args):
        orig(self, input_bundle, ray_remote_args)
        if self.name.startswith("ReadFiles"):
            m = ray_remote_args.get("memory")
            if m is not None:
                captured.append(int(m))

    MapOperator._merge_task_memory_into_ray_remote_args = recorder  # type: ignore[method-assign]

    ctx = DataContext.get_current()
    ctx.use_datasource_v2 = True
    target_max = ctx.target_max_block_size or (128 * 1024 * 1024)

    ray.init(num_cpus=2, ignore_reinit_error=True)
    try:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "t.parquet"
            pq.write_table(pa.table({"c": list(range(100))}), str(p))
            ray.data.read_parquet(str(p.parent)).materialize()
            rg_max = _parquet_max_uncompressed_row_group_bytes(str(p), None)
    finally:
        MapOperator._merge_task_memory_into_ray_remote_args = orig  # type: ignore[method-assign]
        ray.shutdown()

    if not captured:
        print("No ReadFiles memory observations (unexpected).", file=sys.stderr)
        sys.exit(1)

    base = max(int(rg_max), int(target_max))
    expected = base + eps
    print(
        "read_files_task_memory_probe:",
        f"READ_FILES_TASK_MEMORY_EPS_BYTES (module)={eps}",
        f"target_max_block_size={target_max}",
        f"expected_task_memory={expected}",
        f"observed_readfiles_ray_memory_each_task={captured}",
        sep="\n",
    )
    if any(m != expected for m in captured):
        print(
            "Mismatch: workers may be using a different install than this interpreter.",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
