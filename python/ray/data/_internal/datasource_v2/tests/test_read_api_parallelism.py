"""Unit tests for the parallelism (``num_buckets``) decision made by V2's
``read_api`` entry. The decision delegates to ``_autodetect_parallelism``
(also used by V1), so V2 picks up the same CPU-saturation floor.

These tests exercise the helper directly with ``mem_size=None`` —
matching how V2 calls it (no plan-time data-size estimate) — and lock
in three behaviors:

1. ``parallelism != -1`` is honored verbatim (user override).
2. ``parallelism == -1`` with a small cluster falls back to
   ``ctx.read_op_min_num_blocks``.
3. ``parallelism == -1`` with a large cluster honors the CPU floor
   (``2 * avail_cpus``).
"""

from __future__ import annotations

import pytest

from ray.data._internal.util import _autodetect_parallelism
from ray.data.context import DataContext


@pytest.fixture
def ctx() -> DataContext:
    return DataContext.get_current()


def test_num_buckets_honors_user_override(ctx: DataContext):
    """``parallelism != -1`` is returned verbatim."""
    resolved, _reason, _mem_size = _autodetect_parallelism(
        parallelism=50,
        target_max_block_size=ctx.target_max_block_size,
        ctx=ctx,
        mem_size=None,
    )
    assert resolved == 50


def test_num_buckets_falls_back_to_read_op_min_num_blocks_on_small_cluster(
    ctx: DataContext,
):
    """Small cluster + no override → ``read_op_min_num_blocks`` floor wins."""
    resolved, _reason, _mem_size = _autodetect_parallelism(
        parallelism=-1,
        target_max_block_size=ctx.target_max_block_size,
        ctx=ctx,
        mem_size=None,
        avail_cpus=4,  # 2 * 4 = 8 < 200
    )
    assert resolved == ctx.read_op_min_num_blocks


def test_num_buckets_honors_cpu_floor_on_large_cluster(ctx: DataContext):
    """Large cluster + no override → CPU floor (2 * avail_cpus) wins.

    This is the V1 parity guarantee Option 1 introduces to V2:
    downstream operators with ``concurrency=N`` are never starved by a
    read stage that produced fewer than ~2N blocks.
    """
    resolved, _reason, _mem_size = _autodetect_parallelism(
        parallelism=-1,
        target_max_block_size=ctx.target_max_block_size,
        ctx=ctx,
        mem_size=None,
        avail_cpus=256,  # 2 * 256 = 512 > 200 default floor
    )
    assert resolved == 512


def test_num_buckets_size_floors_inert_when_mem_size_none(ctx: DataContext):
    """With ``mem_size=None`` (V2's call shape), the helper's
    ``min_safe_parallelism`` / ``max_reasonable_parallelism`` branches
    stay at identity defaults; result depends only on
    ``read_op_min_num_blocks`` and ``2 * avail_cpus``."""
    resolved, _reason, _mem_size = _autodetect_parallelism(
        parallelism=-1,
        target_max_block_size=ctx.target_max_block_size,
        ctx=ctx,
        mem_size=None,
        avail_cpus=100,  # 2 * 100 = 200, tied with default floor
    )
    assert resolved == max(ctx.read_op_min_num_blocks, 200)


# ---------------------------------------------------------------------------
# ParquetFileReader batch-size knob (Fix 2)
# ---------------------------------------------------------------------------


@pytest.fixture
def reset_ctx_reader_batch_knob():
    """Clear ``parquet_reader_target_batch_size_bytes`` after the test."""
    ctx = DataContext.get_current()
    original = ctx.parquet_reader_target_batch_size_bytes
    ctx.parquet_reader_target_batch_size_bytes = None
    try:
        yield ctx
    finally:
        ctx.parquet_reader_target_batch_size_bytes = original


def test_resolve_batch_target_bytes_falls_back_to_target_min_block_size(
    reset_ctx_reader_batch_knob: DataContext,
):
    """When the new knob is unset, fall back to ``target_min_block_size``."""
    from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
        ParquetFileReader,
    )

    ctx = reset_ctx_reader_batch_knob
    reader = ParquetFileReader(filesystem=None)
    assert reader._resolve_batch_target_bytes() == ctx.target_min_block_size


def test_resolve_batch_target_bytes_uses_knob_when_set(
    reset_ctx_reader_batch_knob: DataContext,
):
    """Knob value takes precedence over the ``target_min_block_size`` fallback."""
    from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
        ParquetFileReader,
    )

    ctx = reset_ctx_reader_batch_knob
    ctx.parquet_reader_target_batch_size_bytes = 64 * 1024  # 64 KiB
    reader = ParquetFileReader(filesystem=None)
    assert reader._resolve_batch_target_bytes() == 64 * 1024


def test_resolve_batch_target_bytes_none_when_both_unset(
    reset_ctx_reader_batch_knob: DataContext,
):
    """Both knob and ``target_min_block_size`` unset → returns None."""
    from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
        ParquetFileReader,
    )

    ctx = reset_ctx_reader_batch_knob
    original_min = ctx.target_min_block_size
    ctx.target_min_block_size = None
    try:
        reader = ParquetFileReader(filesystem=None)
        assert reader._resolve_batch_target_bytes() is None
    finally:
        ctx.target_min_block_size = original_min


# ---------------------------------------------------------------------------
# ReadFiles output block target — _resolve_read_files_output_block_target
# ---------------------------------------------------------------------------


@pytest.fixture
def reset_ctx_output_block_knob():
    """Clear ``parquet_reader_target_output_block_size_bytes`` after the test."""
    ctx = DataContext.get_current()
    original = ctx.parquet_reader_target_output_block_size_bytes
    ctx.parquet_reader_target_output_block_size_bytes = None
    try:
        yield ctx
    finally:
        ctx.parquet_reader_target_output_block_size_bytes = original


def test_output_block_target_falls_back_to_target_min_block_size(
    reset_ctx_output_block_knob: DataContext,
):
    """Knob unset → fall back to ``target_min_block_size``."""
    from ray.data._internal.planner.plan_read_files_op import (
        _resolve_read_files_output_block_target,
    )

    ctx = reset_ctx_output_block_knob
    target = _resolve_read_files_output_block_target(ctx)
    assert target == ctx.target_min_block_size


def test_output_block_target_uses_knob_when_set(
    reset_ctx_output_block_knob: DataContext,
):
    """Knob value wins over the ``target_min_block_size`` fallback."""
    from ray.data._internal.planner.plan_read_files_op import (
        _resolve_read_files_output_block_target,
    )

    ctx = reset_ctx_output_block_knob
    ctx.parquet_reader_target_output_block_size_bytes = 4 * 1024 * 1024  # 4 MiB
    target = _resolve_read_files_output_block_target(ctx)
    assert target == 4 * 1024 * 1024


def test_output_block_target_last_resort_is_target_max_block_size(
    reset_ctx_output_block_knob: DataContext,
):
    """Both knob and ``target_min_block_size`` unset → fall back to
    ``target_max_block_size`` (preserves prior coalescing behavior when
    ``target_min_block_size`` is explicitly disabled)."""
    from ray.data._internal.planner.plan_read_files_op import (
        _resolve_read_files_output_block_target,
    )

    ctx = reset_ctx_output_block_knob
    original_min = ctx.target_min_block_size
    ctx.target_min_block_size = None
    try:
        target = _resolve_read_files_output_block_target(ctx)
        assert target == ctx.target_max_block_size
    finally:
        ctx.target_min_block_size = original_min


def test_output_block_target_knob_wins_even_when_target_min_block_size_set(
    reset_ctx_output_block_knob: DataContext,
):
    """User-set knob always wins regardless of the other size knobs."""
    from ray.data._internal.planner.plan_read_files_op import (
        _resolve_read_files_output_block_target,
    )

    ctx = reset_ctx_output_block_knob
    # Configure all three size knobs to distinct values; assert the
    # output-block knob is honored.
    original_min = ctx.target_min_block_size
    original_max = ctx.target_max_block_size
    ctx.target_min_block_size = 7 * 1024 * 1024
    ctx.target_max_block_size = 99 * 1024 * 1024
    ctx.parquet_reader_target_output_block_size_bytes = 2 * 1024 * 1024
    try:
        assert _resolve_read_files_output_block_target(ctx) == 2 * 1024 * 1024
    finally:
        ctx.target_min_block_size = original_min
        ctx.target_max_block_size = original_max


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
