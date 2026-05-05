"""Integration checks: DataSource V2 ReadFiles merges manifest ``task_memory_bytes``
(including :data:`READ_FILES_TASK_MEMORY_EPS_BYTES`) into Ray task ``memory``.

These tests exercise the full ListFiles → manifest enrichment → ReadFiles scheduling
path. Manifest enrichment runs in Ray workers, which resolve ``eps`` at import from
``RAY_DATA_READ_FILES_TASK_MEMORY_EPS_BYTES`` or the built-in default — do not assert
on a driver-only ``monkeypatch`` of ``READ_FILES_TASK_MEMORY_EPS_BYTES`` here.

For tuning and probes, see ``README_read_files_task_memory.md`` and
``read_files_task_memory_probe.py``.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.datasource_v2.readers import read_files_task_memory as rftm
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    _parquet_max_uncompressed_row_group_bytes,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def _write_parquet(path: Path, table: pa.Table) -> None:
    pq.write_table(table, str(path))


def _expected_read_files_task_memory_bytes(
    parquet_path: str, target_max_block_size: int, eps: int
) -> int:
    """Match :func:`estimate_read_files_task_memory_bytes` for a single local file."""
    rg = _parquet_max_uncompressed_row_group_bytes(parquet_path, None)
    tmax = int(target_max_block_size)
    base = max(int(rg), 2 * tmax)
    return base + int(eps)


@pytest.fixture
def restore_ctx():
    ctx = DataContext.get_current()
    original_v2 = ctx.use_datasource_v2
    original_tmax = ctx.target_max_block_size
    try:
        yield ctx
    finally:
        ctx.use_datasource_v2 = original_v2
        ctx.target_max_block_size = original_tmax


def _merge_capture_wrapper(captured: List[int]):
    """Wrap ``_merge_task_memory_into_ray_remote_args`` to record ReadFiles memory."""

    orig = MapOperator._merge_task_memory_into_ray_remote_args

    def wrapped(self, input_bundle, ray_remote_args):
        orig(self, input_bundle, ray_remote_args)
        if self.name.startswith("ReadFiles"):
            mem = ray_remote_args.get("memory")
            if mem is not None:
                captured.append(int(mem))

    return wrapped


def test_read_parquet_v2_merged_ray_memory_matches_task_estimate(
    ray_start_regular_shared,
    tmp_path: Path,
    restore_ctx: DataContext,
    monkeypatch: pytest.MonkeyPatch,
):
    """End-to-end: Ray ``memory`` on ReadFiles tasks reflects ``base + eps``.

    Manifest enrichment runs in Ray workers, so ``READ_FILES_TASK_MEMORY_EPS_BYTES``
    must match the value those processes resolve at import (from
    ``RAY_DATA_READ_FILES_TASK_MEMORY_EPS_BYTES`` or the built-in default). Do not
    rely on driver-only ``monkeypatch`` of that name here.
    """
    captured: List[int] = []
    monkeypatch.setattr(
        MapOperator,
        "_merge_task_memory_into_ray_remote_args",
        _merge_capture_wrapper(captured),
    )

    restore_ctx.use_datasource_v2 = True
    target_max = 3 * 1024 * 1024
    restore_ctx.target_max_block_size = target_max

    data_path = tmp_path / "data.parquet"
    _write_parquet(data_path, pa.table({"x": list(range(10))}))

    ds = ray.data.read_parquet(str(tmp_path))
    ds.materialize()

    assert captured, "expected at least one ReadFiles task to merge task memory"
    eps = int(rftm.READ_FILES_TASK_MEMORY_EPS_BYTES)
    expected = _expected_read_files_task_memory_bytes(str(data_path), target_max, eps)
    assert all(m == expected for m in captured), (captured, expected)


def test_read_parquet_v2_larger_target_max_block_size_raises_ray_memory_reservation(
    ray_start_regular_shared,
    tmp_path: Path,
    restore_ctx: DataContext,
    monkeypatch: pytest.MonkeyPatch,
):
    """Larger ``DataContext.target_max_block_size`` raises merged ``memory`` (workers see it).

    Varying ``READ_FILES_TASK_MEMORY_EPS_BYTES`` cannot be asserted from a driver-only
    ``monkeypatch`` because ListFiles tasks run in other processes and read ``eps`` at
    import. ``target_max_block_size`` is carried on the serialized
    :class:`~ray.data.context.DataContext` ref, so it is safe to compare two reads.
    """
    captured: List[int] = []
    monkeypatch.setattr(
        MapOperator,
        "_merge_task_memory_into_ray_remote_args",
        _merge_capture_wrapper(captured),
    )

    restore_ctx.use_datasource_v2 = True

    d1 = tmp_path / "d1"
    d2 = tmp_path / "d2"
    d1.mkdir()
    d2.mkdir()
    table = pa.table({"a": [1, 2, 3]})
    _write_parquet(d1 / "a.parquet", table)
    _write_parquet(d2 / "b.parquet", table)

    t_small = 2 * 1024 * 1024
    t_large = 96 * 1024 * 1024
    eps = int(rftm.READ_FILES_TASK_MEMORY_EPS_BYTES)

    restore_ctx.target_max_block_size = t_small
    captured.clear()
    ray.data.read_parquet(str(d1)).materialize()
    assert captured
    mem_small = max(captured)

    restore_ctx.target_max_block_size = t_large
    captured.clear()
    ray.data.read_parquet(str(d2)).materialize()
    assert captured
    mem_large = max(captured)

    assert mem_small < mem_large
    parquet_path = str(d2 / "b.parquet")
    rg = int(_parquet_max_uncompressed_row_group_bytes(parquet_path, None))
    base_small = max(rg, 2 * t_small)
    base_large = max(rg, 2 * t_large)
    assert mem_small == base_small + eps
    assert mem_large == base_large + eps
    assert mem_large - mem_small == base_large - base_small


def test_read_parquet_v2_user_ray_memory_is_respected_when_larger_than_hint(
    ray_start_regular_shared,
    tmp_path: Path,
    restore_ctx: DataContext,
    monkeypatch: pytest.MonkeyPatch,
):
    """``max(user memory, task estimate)`` — user wins when already larger."""
    captured: List[int] = []
    monkeypatch.setattr(
        MapOperator,
        "_merge_task_memory_into_ray_remote_args",
        _merge_capture_wrapper(captured),
    )

    restore_ctx.use_datasource_v2 = True
    restore_ctx.target_max_block_size = 2 * 1024 * 1024

    _write_parquet(tmp_path / "p.parquet", pa.table({"z": [1]}))

    user_floor = 500_000_000
    ds = ray.data.read_parquet(str(tmp_path), ray_remote_args={"memory": user_floor})
    ds.materialize()

    assert captured
    assert all(m == user_floor for m in captured), captured


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-xvs"]))
