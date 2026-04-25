from unittest.mock import MagicMock

import pytest

import ray
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)


def test_min_max_resource_requirements(ray_start_regular_shared, restore_data_context):
    data_context = ray.data.DataContext.get_current()
    op = TaskPoolMapOperator(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        ray_remote_args={"num_cpus": 1},
    )
    op._metrics = MagicMock(obj_store_mem_max_pending_output_per_task=3)

    (
        min_resource_usage_bound,
        max_resource_usage_bound,
    ) = op.min_max_resource_requirements()

    # At a minimum, you need enough processors to run one task and enough object
    # store memory for a pending task.
    assert min_resource_usage_bound == ExecutionResources(
        cpu=1, gpu=0, object_store_memory=3
    )
    # For CPU-only operators, max GPU/memory is 0 (not inf) to prevent hoarding.
    assert max_resource_usage_bound == ExecutionResources.for_limits(gpu=0, memory=0)


def test_cached_dispatch_options_enabled_without_remote_args_fn(
    ray_start_regular_shared, restore_data_context
):
    """Without a dynamic `ray_remote_args_fn`, the per-bundle-size dispatch
    options should be pre-built once at __init__ rather than rebuilt per
    task. This avoids a per-dispatch `copy.deepcopy(self._ray_remote_args)`
    plus a fresh `self._map_task.options(...)` wrapping.
    """
    data_context = ray.data.DataContext.get_current()
    op = TaskPoolMapOperator(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        ray_remote_args={"num_cpus": 1},
    )
    assert op._cached_small_options is not None
    assert op._cached_large_options is not None
    # Distinct wrappers for the two paths — different scheduling strategies.
    small = op._cached_small_args
    large = op._cached_large_args
    assert small["scheduling_strategy"] != large["scheduling_strategy"]
    assert small["name"] == op.name
    assert large["name"] == op.name


def test_cached_dispatch_options_disabled_with_remote_args_fn(
    ray_start_regular_shared, restore_data_context
):
    """When the user supplies a dynamic `ray_remote_args_fn`, the callback
    may return different args on each call, so the per-task rebuild path
    must still apply. The cache must not be populated in that case.
    """
    data_context = ray.data.DataContext.get_current()
    op = TaskPoolMapOperator(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        ray_remote_args={"num_cpus": 1},
        ray_remote_args_fn=lambda: {"num_cpus": 2},
    )
    assert op._cached_small_options is None
    assert op._cached_large_options is None


def _make_op(ray_remote_args=None, ray_remote_args_fn=None, name="MapOp"):
    """Build a TaskPoolMapOperator with sensible defaults for the
    regression tests below."""
    data_context = ray.data.DataContext.get_current()
    return TaskPoolMapOperator(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        name=name,
        ray_remote_args=ray_remote_args or {"num_cpus": 1},
        ray_remote_args_fn=ray_remote_args_fn,
    )


def _bundle_with_size(size_bytes):
    """Mock RefBundle whose `size_bytes()` returns the given value.

    The fast path consults `bundle.size_bytes()` once to choose the
    small vs large cache. Mocking is sufficient for tests that don't
    actually dispatch the task.
    """
    b = MagicMock()
    b.size_bytes = MagicMock(return_value=size_bytes)
    return b


def test_post_init_ray_remote_args_fn_routes_to_slow_path(
    ray_start_regular_shared, restore_data_context
):
    """``ConfigureMapTaskMemoryRule.apply`` installs
    ``op._ray_remote_args_fn = ray_remote_args_fn`` AFTER the operator
    is constructed, to inject per-task ``memory`` for OOM protection.
    The fast path must detect the post-init callback and route to the
    slow path so the callback is invoked on every dispatch — otherwise
    the cache hit silently disables memory configuration for every
    ``MapOperator`` in the pipeline.
    """
    op = _make_op(ray_remote_args={"num_cpus": 1})
    # Cache is built (ray_remote_args_fn was None at __init__).
    assert op._cached_small_options is not None

    invocations = []

    def post_init_callback():
        invocations.append(1)
        return {"memory": 1024 * 1024 * 100}  # 100 MB

    # Simulate ConfigureMapTaskMemoryRule installing a callback post-init.
    op._ray_remote_args_fn = post_init_callback

    captured_options_kwargs = []

    def _mocked_options(**kwargs):
        captured_options_kwargs.append(kwargs)
        wrapper = MagicMock()
        wrapper.remote = MagicMock(return_value=MagicMock())
        return wrapper

    op._map_task.options = _mocked_options

    op._try_schedule_task(_bundle_with_size(1), strict=True)

    assert len(invocations) == 1, (
        "Post-init _ray_remote_args_fn must be invoked on dispatch; "
        "the fast-path cache hit must not silently disable it."
    )
    # And the resulting dispatch options must include the callback's
    # injected memory configuration — proving the fast path was bypassed
    # in favor of the slow path that calls the callback.
    assert captured_options_kwargs[-1].get("memory") == 1024 * 1024 * 100


def test_metrics_snapshot_excludes_dispatch_only_keys_for_large_bundle(
    ray_start_regular_shared, restore_data_context
):
    """Pristine's ``_remote_args_for_metrics`` is a deep copy of
    ``ray_remote_args`` taken inside ``_get_dynamic_ray_remote_args``
    BEFORE ``name`` and ``_generator_backpressure_num_objects`` are
    added by ``_try_schedule_task``. The cached path must produce the
    same key set in the metrics snapshot — the dispatch-only keys
    must not leak into the user-visible ``Dataset.stats()`` output.
    """
    op = _make_op(ray_remote_args={"num_cpus": 1})
    threshold = op.data_context.large_args_threshold

    def _mocked_options(**kwargs):
        wrapper = MagicMock()
        wrapper.remote = MagicMock(return_value=MagicMock())
        return wrapper

    op._map_task.options = _mocked_options

    op._try_schedule_task(_bundle_with_size(threshold * 2), strict=True)
    keys = set(op._remote_args_for_metrics.keys())

    assert "name" not in keys
    assert "_generator_backpressure_num_objects" not in keys
    assert keys == {"num_cpus", "scheduling_strategy"}


def test_user_pinned_strategy_metrics_match_pristine(
    ray_start_regular_shared, restore_data_context
):
    """When the user pins ``scheduling_strategy`` via ``ray_remote_args``,
    pristine's ``_get_dynamic_ray_remote_args`` skips the entire
    ``"scheduling_strategy" not in ray_remote_args`` branch and never
    re-snapshots metrics. The cached path's metrics snapshot for both
    small and large dispatches must be byte-equivalent to pristine's
    initialization snapshot in that case.
    """
    user_args = {"num_cpus": 2, "scheduling_strategy": "DEFAULT"}
    op = _make_op(ray_remote_args=user_args)
    threshold = op.data_context.large_args_threshold

    def _mocked_options(**kwargs):
        wrapper = MagicMock()
        wrapper.remote = MagicMock(return_value=MagicMock())
        return wrapper

    op._map_task.options = _mocked_options

    op._try_schedule_task(_bundle_with_size(threshold * 2), strict=True)

    pristine_initial_snapshot = {"num_cpus": 2, "scheduling_strategy": "DEFAULT"}
    assert dict(op._remote_args_for_metrics) == pristine_initial_snapshot


# --- Latent-divergence pins ---------------------------------------
#
# These tests pin specific divergences from pristine that the cache
# introduces. The cache assumes certain inputs are immutable post-init
# (the operator's `_ray_remote_args`, the data context's
# `scheduling_strategy` and `_max_num_blocks_in_streaming_gen_buffer`).
# Production code does not mutate these fields post-init, so the
# divergence is unreachable today. The tests pin the current shape so
# a future change that mutates one of these fields fails loudly and
# forces a conscious cache-invalidation decision.


def test_m3_caches_scheduling_strategy_at_init_diverges_from_pristine_on_mutation(
    ray_start_regular_shared, restore_data_context
):
    """M3 caches
    ``data_context.scheduling_strategy`` at ``__init__``. Pristine
    re-reads ``self.data_context.scheduling_strategy`` per dispatch.
    Mutating the context after the op exists produces different
    args-dict outputs.

    Severity: latent — `grep -rn "\\.scheduling_strategy = "
    python/ray/data/` shows no production code mutates this field
    post-init. Documented to preserve the snapshot semantics
    intentionally.
    """
    ctx = ray.data.DataContext.get_current()
    op = _make_op(ray_remote_args={"num_cpus": 1})
    initial_cached = op._cached_small_args["scheduling_strategy"]

    ctx.scheduling_strategy = "POST_INIT_MUTATION"

    # Pristine reflects the new strategy on the next dispatch:
    pristine_args = op._get_dynamic_ray_remote_args(input_bundle=_bundle_with_size(1))
    assert pristine_args["scheduling_strategy"] == "POST_INIT_MUTATION"

    # M3 cache is frozen at the __init__ value — divergence:
    assert op._cached_small_args["scheduling_strategy"] == initial_cached
    assert (
        op._cached_small_args["scheduling_strategy"]
        != pristine_args["scheduling_strategy"]
    ), (
        "Expected divergence between M3's cached strategy and pristine "
        "after a post-init data_context.scheduling_strategy mutation. "
        "If this test fails, the divergence has been eliminated — "
        "consciously update the audit doc and remove this test."
    )


def test_m3_caches_max_num_blocks_in_streaming_gen_buffer_at_init(
    ray_start_regular_shared, restore_data_context
):
    """M3 caches
    ``2 * data_context._max_num_blocks_in_streaming_gen_buffer`` into
    ``_generator_backpressure_num_objects`` at ``__init__``. Pristine
    re-reads per dispatch. Mutation post-init isn't reflected.
    """
    ctx = ray.data.DataContext.get_current()
    initial = ctx._max_num_blocks_in_streaming_gen_buffer
    if initial is None:
        ctx._max_num_blocks_in_streaming_gen_buffer = 2
        initial = 2
    op = _make_op(ray_remote_args={"num_cpus": 1})
    cached_gen = op._cached_small_args.get("_generator_backpressure_num_objects")
    assert cached_gen == 2 * initial

    # Mutate
    ctx._max_num_blocks_in_streaming_gen_buffer = initial + 50

    # Pristine would compute the new value:
    pristine_args = op._get_dynamic_ray_remote_args(input_bundle=_bundle_with_size(1))
    pristine_gen_expected = 2 * (initial + 50)
    # _get_dynamic_ray_remote_args does NOT add _generator_backpressure_num_objects;
    # only the surrounding _try_schedule_task does that. So pristine via the
    # full path would compute pristine_gen_expected; M3's cached value is stale.
    assert op._cached_small_args["_generator_backpressure_num_objects"] != pristine_gen_expected, (
        "Expected divergence. If this test fails, the cache no longer "
        "snapshots _max_num_blocks_in_streaming_gen_buffer at __init__."
    )


def test_m3_caches_ray_remote_args_at_init(
    ray_start_regular_shared, restore_data_context
):
    """M3 calls
    ``copy.deepcopy(self._ray_remote_args)`` once at ``__init__``.
    Pristine deep-copies per dispatch. Mutating ``op._ray_remote_args``
    post-init isn't reflected.

    Severity: latent — production code never mutates an op's
    ``_ray_remote_args`` post-init (verified via
    `grep -rn "_ray_remote_args\\[" python/ray/data/` — only local
    variables, never the op attribute).
    """
    op = _make_op(ray_remote_args={"num_cpus": 1})
    assert op._cached_small_args["num_cpus"] == 1

    op._ray_remote_args["num_cpus"] = 99

    # Pristine reflects the new value on the next dispatch:
    pristine_args = op._get_dynamic_ray_remote_args(input_bundle=_bundle_with_size(1))
    assert pristine_args["num_cpus"] == 99

    # M3 cache is frozen — divergence:
    assert op._cached_small_args["num_cpus"] == 1
    assert op._cached_small_args["num_cpus"] != pristine_args["num_cpus"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
