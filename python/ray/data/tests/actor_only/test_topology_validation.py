from unittest.mock import MagicMock

import pytest

import ray
from ray.data._internal.execution.execution_flags import actor_only_backend_enabled
from ray.data._internal.execution.interfaces import ExecutionOptions
from ray.data._internal.experimental.execution.topology_validation import (
    validate_actor_only_supported_topology,
)
from ray.data._internal.gpu_shuffle.hash_aggregate import GPUHashAggregateOperator
from ray.data._internal.gpu_shuffle.hash_shuffle import GPUShuffleOperator
from ray.data.aggregate import Count

_UNSUPPORTED_ERROR = "is not supported when actor only backend is enabled"
_PRESERVE_ORDER_ERROR = "preserve_order=True is not supported for actor only backend"


def test_preserve_order_rejected_with_actor_only_backend(
    ray_start_2_cpus_shared, restore_data_context
):
    assert actor_only_backend_enabled()
    ray.data.DataContext.get_current().execution_options.preserve_order = True
    with pytest.raises(ValueError, match=_PRESERVE_ORDER_ERROR):
        ray.data.range(10).materialize()


def test_preserve_order_false_allowed_with_actor_only_backend(
    ray_start_2_cpus_shared, restore_data_context
):
    assert actor_only_backend_enabled()
    ray.data.DataContext.get_current().execution_options.preserve_order = False
    assert ray.data.range(10).materialize().count() == 10


@pytest.mark.parametrize(
    "build_op",
    [
        pytest.param(lambda ds: ds.random_shuffle(), id="random_shuffle"),
        pytest.param(lambda ds: ds.randomize_block_order(), id="randomize_block_order"),
        pytest.param(lambda ds: ds.repartition(2), id="repartition"),
        pytest.param(
            lambda ds: ds.repartition(2, shuffle=True), id="repartition_shuffle"
        ),
        pytest.param(lambda ds: ds.sort("id"), id="sort"),
        pytest.param(
            lambda ds: ds.groupby("id").aggregate(Count()), id="groupby_aggregate"
        ),
        pytest.param(lambda ds: ds.groupby("id").count(), id="groupby_count"),
        pytest.param(
            lambda ds: ds.groupby("id").map_groups(lambda g: g), id="groupby_map_groups"
        ),
        pytest.param(lambda ds: ds.aggregate(Count()), id="aggregate"),
        pytest.param(lambda ds: ds.zip(ray.data.range(10)), id="zip"),
    ],
)
def test_all_to_all_apis_rejected_with_actor_only_backend(
    ray_start_2_cpus_shared, build_op
):
    """Unsupported shuffle ops are rejected when the executor boots the topology."""
    assert actor_only_backend_enabled()
    ds = ray.data.range(10)
    with pytest.raises(ValueError, match=_UNSUPPORTED_ERROR):
        result = build_op(ds)
        # Eager APIs (e.g. Dataset.aggregate) execute inside build_op; lazy ones
        # need an action to trigger executor bootstrap / topology validation.
        if isinstance(result, ray.data.Dataset):
            result.materialize()


@pytest.mark.parametrize(
    "op_cls",
    [GPUShuffleOperator, GPUHashAggregateOperator],
    ids=["gpu_shuffle", "gpu_hash_aggregate"],
)
def test_gpu_shuffle_ops_rejected_with_actor_only_backend(op_cls):
    """GPU shuffles run their own actor pool, so the actor-only backend rejects them.

    Building a real GPU shuffle needs GPUs and RAPIDS, so validate a topology
    holding a bare instance instead.
    """
    op = op_cls.__new__(op_cls)
    with pytest.raises(ValueError, match=_UNSUPPORTED_ERROR):
        validate_actor_only_supported_topology({op: MagicMock()}, ExecutionOptions())


def test_streaming_repartition_allowed_with_actor_only_backend(
    ray_start_2_cpus_shared,
):
    """Streaming repartition is a map op, not an AllToAll shuffle."""
    assert actor_only_backend_enabled()
    ds = ray.data.range(10).repartition(target_num_rows_per_block=5)
    assert ds.materialize().count() == 10


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
