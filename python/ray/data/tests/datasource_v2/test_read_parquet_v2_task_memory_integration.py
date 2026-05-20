"""Integration checks: DataSource V2 ReadFiles merges manifest ``task_memory_bytes``
into Ray task ``memory``.

These tests exercise the full ListFiles → manifest enrichment → ReadFiles
scheduling path. ``eps`` is resolved at enrich time from
``DataContext.read_files_task_memory_eps_bytes``, which is serialized on the
``DataContext`` ref so driver-side overrides propagate to workers.

Formula:
    ``max(2 * target_max_block_size, max(file_sizes)) + eps``

For empirical tuning of ``eps``, see ``read_files_task_memory_probe.py``.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def _write_parquet(path: Path, table: pa.Table) -> None:
    pq.write_table(table, str(path))


def _expected_read_files_task_memory_bytes(
    parquet_paths: List[Path], target_max_block_size: int, eps: int
) -> int:
    """Match :func:`estimate_read_files_task_memory_bytes` for a single-task read."""
    max_file = max(int(p.stat().st_size) for p in parquet_paths)
    base = max(2 * int(target_max_block_size), max_file)
    return base + int(eps)


@pytest.fixture
def restore_ctx():
    ctx = DataContext.get_current()
    original_v2 = ctx.use_datasource_v2
    original_tmax = ctx.target_max_block_size
    original_eps = ctx.read_files_task_memory_eps_bytes
    try:
        yield ctx
    finally:
        ctx.use_datasource_v2 = original_v2
        ctx.target_max_block_size = original_tmax
        ctx.read_files_task_memory_eps_bytes = original_eps


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
    """End-to-end: Ray ``memory`` on ReadFiles tasks equals
    ``max(2 * target, max(file_sizes)) + eps``."""
    captured: List[int] = []
    monkeypatch.setattr(
        MapOperator,
        "_merge_task_memory_into_ray_remote_args",
        _merge_capture_wrapper(captured),
    )

    restore_ctx.use_datasource_v2 = True
    target_max = 3 * 1024 * 1024
    restore_ctx.target_max_block_size = target_max
    eps = int(restore_ctx.read_files_task_memory_eps_bytes)

    data_path = tmp_path / "data.parquet"
    _write_parquet(data_path, pa.table({"x": list(range(10))}))

    ds = ray.data.read_parquet(str(tmp_path))
    ds.materialize()

    assert captured, "expected at least one ReadFiles task to merge task memory"
    expected = _expected_read_files_task_memory_bytes([data_path], target_max, eps)
    assert all(m == expected for m in captured), (captured, expected)


def test_read_parquet_v2_larger_target_max_block_size_raises_ray_memory_reservation(
    ray_start_regular_shared,
    tmp_path: Path,
    restore_ctx: DataContext,
    monkeypatch: pytest.MonkeyPatch,
):
    """Larger ``DataContext.target_max_block_size`` -> higher merged ``memory``."""
    captured: List[int] = []
    monkeypatch.setattr(
        MapOperator,
        "_merge_task_memory_into_ray_remote_args",
        _merge_capture_wrapper(captured),
    )

    restore_ctx.use_datasource_v2 = True
    eps = int(restore_ctx.read_files_task_memory_eps_bytes)

    d1 = tmp_path / "d1"
    d2 = tmp_path / "d2"
    d1.mkdir()
    d2.mkdir()
    table = pa.table({"a": [1, 2, 3]})
    _write_parquet(d1 / "a.parquet", table)
    _write_parquet(d2 / "b.parquet", table)

    t_small = 2 * 1024 * 1024
    t_large = 96 * 1024 * 1024

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

    file_b_size = int((d2 / "b.parquet").stat().st_size)
    assert mem_small == max(2 * t_small, file_b_size) + eps
    assert mem_large == max(2 * t_large, file_b_size) + eps
    # 2 * t_large dominates over the parquet file, so the large-target hint
    # is strictly larger than the small-target hint.
    assert mem_small < mem_large


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


def test_read_parquet_v2_eps_override_via_data_context(
    ray_start_regular_shared,
    tmp_path: Path,
    restore_ctx: DataContext,
    monkeypatch: pytest.MonkeyPatch,
):
    """``DataContext.read_files_task_memory_eps_bytes`` propagates to workers."""
    captured: List[int] = []
    monkeypatch.setattr(
        MapOperator,
        "_merge_task_memory_into_ray_remote_args",
        _merge_capture_wrapper(captured),
    )

    restore_ctx.use_datasource_v2 = True
    target_max = 2 * 1024 * 1024
    restore_ctx.target_max_block_size = target_max
    custom_eps = 7 * 1024 * 1024  # 7 MiB — distinguishable from the 64 MiB default
    restore_ctx.read_files_task_memory_eps_bytes = custom_eps

    data_path = tmp_path / "data.parquet"
    _write_parquet(data_path, pa.table({"x": list(range(10))}))

    ray.data.read_parquet(str(tmp_path)).materialize()

    assert captured
    expected = _expected_read_files_task_memory_bytes(
        [data_path], target_max, custom_eps
    )
    assert all(m == expected for m in captured), (captured, expected, custom_eps)


def test_read_parquet_v1_legacy_path_does_not_set_task_memory_bytes(
    ray_start_regular_shared,
    tmp_path: Path,
    restore_ctx: DataContext,
    monkeypatch: pytest.MonkeyPatch,
):
    """V1 legacy read path does not produce ``ReadFiles`` operators that pass
    through the merge — so the capture wrapper never fires for them.

    Locks in the V2-only gating contract: only ``plan_list_files_op`` attaches
    the postprocess hook that stamps ``task_memory_bytes``.
    """
    captured: List[int] = []
    monkeypatch.setattr(
        MapOperator,
        "_merge_task_memory_into_ray_remote_args",
        _merge_capture_wrapper(captured),
    )

    restore_ctx.use_datasource_v2 = False
    restore_ctx.target_max_block_size = 4 * 1024 * 1024

    _write_parquet(tmp_path / "data.parquet", pa.table({"x": list(range(10))}))

    ds = ray.data.read_parquet(str(tmp_path))
    ds.materialize()

    # The capture filter only records ``ReadFiles`` (V2 op name). V1 uses
    # different operator names (e.g. ``ReadParquet``), so nothing should be
    # captured.
    assert captured == [], (
        "V1 legacy path unexpectedly produced ReadFiles task-memory merges: "
        f"{captured}"
    )


def test_read_parquet_v2_largest_file_drives_hint(
    ray_start_regular_shared,
    tmp_path: Path,
    restore_ctx: DataContext,
    monkeypatch: pytest.MonkeyPatch,
):
    """Heterogeneous file sizes -> only ``max(file_sizes)`` shapes the hint."""
    captured: List[int] = []
    monkeypatch.setattr(
        MapOperator,
        "_merge_task_memory_into_ray_remote_args",
        _merge_capture_wrapper(captured),
    )

    restore_ctx.use_datasource_v2 = True
    target_max = 1 * 1024 * 1024  # 1 MiB — chosen so 2*target won't dominate
    restore_ctx.target_max_block_size = target_max
    eps = int(restore_ctx.read_files_task_memory_eps_bytes)

    # Write two files of very different sizes in the same directory.
    small = tmp_path / "small.parquet"
    big = tmp_path / "big.parquet"
    _write_parquet(small, pa.table({"x": list(range(10))}))
    _write_parquet(big, pa.table({"x": list(range(20_000))}))

    ray.data.read_parquet(str(tmp_path)).materialize()

    assert captured
    expected = _expected_read_files_task_memory_bytes([small, big], target_max, eps)
    # All ReadFiles tasks should see the same hint (max file in the manifest
    # drives it), but at minimum the captured value matches the expected
    # max-file-driven formula.
    assert max(captured) == expected, (captured, expected)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-xvs"]))
