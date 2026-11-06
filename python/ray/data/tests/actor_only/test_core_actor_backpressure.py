import inspect
from typing import Any, Dict, cast

import pytest

import ray
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.execution.execution_flags import (
    CORE_ACTOR_BACKPRESSURE_NUM_OBJECTS_PER_YIELD,
    actor_only_backend_enabled,
)
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    ActorPoolMapOperator,
    _MapWorker,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.util import yield_block_with_stats
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (
    ExperimentalActorPoolMapOperator,
    _configure_map_worker_for_core_backpressure,
)
from ray.data.context import DataContext


def _make_actor_op(
    ray_remote_args: Dict[str, Any],
    generator_backpressure_disabled: bool = False,
    op_cls: type = ExperimentalActorPoolMapOperator,
) -> ActorPoolMapOperator:
    """Build a real actor-pool operator the way the planner does. Defaults to
    the actor-only operator, which owns the actor-wide backpressure config."""
    ctx = DataContext.get_current()
    input_op = InputDataBuffer(ctx, input_data=[])
    transformer = MapTransformer(
        [BlockMapTransformFn(lambda blocks, _ctx: iter(blocks))]
    )
    return op_cls(
        map_transformer=transformer,
        input_op=input_op,
        data_context=ctx,
        compute_strategy=ActorPoolStrategy(size=1),
        ray_remote_args=ray_remote_args,
        name="Infer",
        generator_backpressure_disabled=generator_backpressure_disabled,
    )


def test_map_worker_submit_uses_grouped_yields():
    assert actor_only_backend_enabled()
    ctx = DataContext.get_current()
    worker_cls = _configure_map_worker_for_core_backpressure(
        type("MapWorker(test)", (_MapWorker,), {}),
        ctx,
    )
    assert hasattr(worker_cls.submit, "__ray_num_objects_per_yield__")
    assert (
        worker_cls.submit.__ray_num_objects_per_yield__
        == CORE_ACTOR_BACKPRESSURE_NUM_OBJECTS_PER_YIELD
    )
    assert ctx._use_grouped_streaming_generator_yields
    # The annotation must survive Ray's actor-class introspection (which
    # unwraps ``__wrapped__``): it is what makes core split each grouped yield
    # into block and metadata refs.
    method_meta = cast(Any, ray.remote(worker_cls)).__ray_metadata__.method_meta
    assert (
        method_meta.num_objects_per_yield["submit"]
        == CORE_ACTOR_BACKPRESSURE_NUM_OBJECTS_PER_YIELD
    )


def test_yield_block_with_stats_uses_grouped_yield():
    ctx = DataContext.get_current()
    ctx._use_grouped_streaming_generator_yields = True

    def build_metadata(block_ser_time_s) -> Any:
        # A dict stands in for BlockMetadataWithSchema; the generator only
        # pickles it.
        return {"ser_time": block_ser_time_s}

    with DataContext.current(ctx):
        gen = yield_block_with_stats({"id": [1]}, build_metadata)
        assert inspect.isgenerator(gen)

        output = gen.send(None)
        assert isinstance(output, tuple)
        assert len(output) == CORE_ACTOR_BACKPRESSURE_NUM_OBJECTS_PER_YIELD
        assert output[0] == {"id": [1]}
        assert isinstance(output[1], bytes)

        with pytest.raises(StopIteration):
            gen.send(None)


def test_per_task_backpressure_disabled_on_actor_only_backend():
    """With the actor-wide cap owning backpressure, the per-task budget must be
    off (-1) so the two mechanisms don't compete."""
    op = _make_actor_op({"num_cpus": 1})
    assert op._ray_actor_task_remote_args["_generator_backpressure_num_objects"] == -1


def test_actor_wide_cap_requires_positive_gen_buffer(monkeypatch):
    """The actor-wide cap is sized from the streaming-gen buffer; a non-positive
    buffer is a configuration error, not something to paper over with a default."""
    ctx = DataContext.get_current()
    monkeypatch.setattr(ctx, "_max_num_blocks_in_streaming_gen_buffer", 0)
    with pytest.raises(AssertionError, match="positive block count"):
        _make_actor_op({"num_cpus": 1})


def test_generator_backpressure_disabled_emits_no_keys():
    """A planner that disables generator backpressure for its operator (e.g.
    ListFiles, Download pass ``generator_backpressure_disabled=True``) must
    produce NO backpressure keys at all -- Ray core treats an absent key as
    disabled, and the ResourceBank's prebuffer estimate reads absent as
    disabled too."""
    op = _make_actor_op({"num_cpus": 1}, generator_backpressure_disabled=True)
    assert "_actor_generator_backpressure_num_objects" not in op._ray_remote_args
    assert "_generator_backpressure_num_objects" not in op._ray_actor_task_remote_args


def test_generator_backpressure_disabled_propagates_through_create():
    """``MapOperator.create`` is the seam planners use; the kwarg must reach
    both pool implementations."""
    ctx = DataContext.get_current()
    input_op = InputDataBuffer(ctx, input_data=[])
    transformer = MapTransformer(
        [BlockMapTransformFn(lambda blocks, _ctx: iter(blocks))]
    )

    actor_op = MapOperator.create(
        transformer,
        input_op,
        ctx,
        compute_strategy=ActorPoolStrategy(size=1),
        ray_remote_args={"num_cpus": 1},
        generator_backpressure_disabled=True,
    )
    assert isinstance(actor_op, ActorPoolMapOperator)
    assert "_actor_generator_backpressure_num_objects" not in actor_op._ray_remote_args
    assert (
        "_generator_backpressure_num_objects"
        not in actor_op._ray_actor_task_remote_args
    )

    task_op = MapOperator.create(
        transformer,
        input_op,
        ctx,
        ray_remote_args={"num_cpus": 1},
        generator_backpressure_disabled=True,
    )
    assert task_op._generator_backpressure_disabled


def test_flag_off_keeps_task_level_backpressure(monkeypatch):
    """The base operator carries no actor-only branch: with the backend off,
    the per-task default and args are byte-identical to the standard backend,
    and the opt-out kwarg still suppresses the per-task default."""
    monkeypatch.setenv("RAY_DATA_ACTOR_ONLY_BACKEND", "0")
    assert not actor_only_backend_enabled()

    ctx = DataContext.get_current()
    op = _make_actor_op({"num_cpus": 1}, op_cls=ActorPoolMapOperator)
    assert "_actor_generator_backpressure_num_objects" not in op._ray_remote_args
    assert op._ray_actor_task_remote_args["_generator_backpressure_num_objects"] == (
        2 * ctx._max_num_blocks_in_streaming_gen_buffer
    )
    assert not hasattr(op._map_worker_cls.submit, "__ray_num_objects_per_yield__")

    op = _make_actor_op(
        {"num_cpus": 1},
        generator_backpressure_disabled=True,
        op_cls=ActorPoolMapOperator,
    )
    assert "_actor_generator_backpressure_num_objects" not in op._ray_remote_args
    assert "_generator_backpressure_num_objects" not in op._ray_actor_task_remote_args


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
