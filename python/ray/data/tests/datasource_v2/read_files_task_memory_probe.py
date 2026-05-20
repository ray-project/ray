#!/usr/bin/env python3
"""Probe ReadFiles Ray ``memory`` reservations against the live formula.

Formula:
    ``max(2 * target_max_block_size, max(file_sizes)) + eps``

``DataContext`` is serialized into Ray tasks, so setting
``ctx.read_files_task_memory_eps_bytes`` on the driver propagates to workers.
Use this to sweep ``eps`` values without code edits.

Run from the ``python/`` directory::

    cd python
    python ray/data/tests/datasource_v2/read_files_task_memory_probe.py
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq

import ray
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data.context import DataContext


def main() -> None:
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
    eps = int(ctx.read_files_task_memory_eps_bytes)

    ray.init(num_cpus=2, ignore_reinit_error=True)
    try:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "t.parquet"
            pq.write_table(pa.table({"c": list(range(100))}), str(p))
            ray.data.read_parquet(str(p.parent)).materialize()
            file_size = int(p.stat().st_size)
    finally:
        MapOperator._merge_task_memory_into_ray_remote_args = orig  # type: ignore[method-assign]
        ray.shutdown()

    if not captured:
        print("No ReadFiles memory observations (unexpected).", file=sys.stderr)
        sys.exit(1)

    base = max(2 * int(target_max), file_size)
    expected = base + eps
    print(
        "read_files_task_memory_probe:",
        f"DataContext.read_files_task_memory_eps_bytes={eps}",
        f"target_max_block_size={target_max}",
        f"max(file_sizes)={file_size}",
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
