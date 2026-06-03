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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
