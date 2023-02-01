from typing import Callable, List, Iterator

from ray.data.block import Block
from ray.data._internal.compute import get_compute
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import TaskContext, MapTransformFn
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import (
    AbstractUDFMap,
    MapBatches,
    MapRows,
    FlatMap,
    Filter,
    Write,
)
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.metrics import MetricsCollector
from ray.data._internal.planner.transforms.adapters import (
    InputAdapter,
    OutputAdapter,
    DataT,
)
from ray.data._internal.planner.transforms.filter import generate_filter_transform
from ray.data._internal.planner.transforms.flat_map import generate_flat_map_transform
from ray.data._internal.planner.transforms.map_batches import (
    generate_map_batches_transform,
)
from ray.data._internal.planner.transforms.map_rows import generate_map_rows_transform
from ray.data._internal.planner.transforms.read import generate_read_transform
from ray.data._internal.planner.transforms.write import generate_write_transform
from ray.data._internal.planner.transforms.utils import _wrap_callable_class


TransformT = Callable[[Iterator[DataT], TaskContext], Iterator[DataT]]
AdapterT = Callable[[Iterator[DataT]], Iterator[DataT]]


def generate_block_transform_for_op(op: LogicalOperator) -> MapTransformFn:
    """
    Create an Iterator[Block] -> Iterator[Block] transformation from the provided
    logical operator that the physical MapOperator can work with.
    """
    data_transform = generate_data_transform_for_op(op)
    input_adapter = InputAdapter.from_downstream_op(op)
    output_adapter = OutputAdapter.from_upstream_op(op)
    return generate_adapted_transform([data_transform], [input_adapter, output_adapter])


def generate_data_transform_for_op(op: LogicalOperator) -> TransformT:
    """
    Create an Iterator[Data] -> Iterator[Data] transformation from the provided
    logical operator.

    This is used when manually constructing a block transform via one or more data
    transforms and adapters, e.g. during operator fusion.
    """
    if isinstance(op, AbstractUDFMap):
        compute = get_compute(op._compute)
        fn = _wrap_callable_class(
            op._fn, compute, op._fn_constructor_args, op._fn_constructor_kwargs
        )
        if isinstance(op, MapBatches):
            return generate_map_batches_transform(fn, op._fn_args, op._fn_kwargs)
        elif isinstance(op, MapRows):
            return generate_map_rows_transform(fn)
        elif isinstance(op, FlatMap):
            return generate_flat_map_transform(fn)
        elif isinstance(op, Filter):
            return generate_filter_transform(fn)
        elif isinstance(op, Write):
            return generate_write_transform(op._datasource, **op._write_args)
    elif isinstance(op, Read):
        return generate_read_transform()

    raise ValueError(f"Found unknown logical operator during planning: {op}")


def generate_adapted_transform(
    transforms: List[TransformT], adapters: List[AdapterT]
) -> MapTransformFn:
    """Take the N transforms and N+1 adapters and construct a block transform.

    There must be one more adapter than transforms, and these transforms and adapters
    will be interleaved as follows:
        a_0() -> t_0() -> a_1() -> t_1() -> ... -> a_n() -> t_n() -> a_{n+1}()
    """
    assert len(adapters) == len(transforms) + 1

    def adapted_transform(
        blocks: Iterator[Block],
        ctx: TaskContext,
        metrics_collector: MetricsCollector,
    ) -> Iterator[Block]:
        adapters[0].register_metrics_collector(metrics_collector)
        data = adapters[0].adapt(blocks)
        for transform, adapter in zip(transforms, adapters[1:]):
            data = transform(data, ctx)
            adapter.register_metrics_collector(metrics_collector)
            data = adapter.adapt(data)
        did_yield = False
        for datum in data:
            did_yield = True
            yield datum
        if not did_yield:
            # Build an empty block from the last adapter that saw data.
            for adapter in reversed(adapters):
                builder = adapter.builder()
                if builder is not None:
                    yield builder.build()
                    break
            else:
                # If no data seen, fall back to delegating block builder fallback.
                yield DelegatingBlockBuilder().build()

    return adapted_transform
