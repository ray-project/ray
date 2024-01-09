import collections
from types import GeneratorType
from typing import Any, Callable, Iterable, Iterator, Optional

import numpy as np
import pandas as pd
import pyarrow as pa

import ray
from ray.data._internal.compute import get_compute
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BatchMapTransformFn,
    BlocksToBatchesMapTransformFn,
    BlocksToRowsMapTransformFn,
    BuildOutputBlocksMapTransformFn,
    MapTransformCallable,
    MapTransformer,
    Row,
    RowMapTransformFn,
)
from ray.data._internal.execution.util import make_callable_class_concurrent
from ray.data._internal.logical.operators.map_operator import (
    AbstractUDFMap,
    Filter,
    FlatMap,
    MapBatches,
    MapRows,
)
from ray.data._internal.numpy_support import is_valid_udf_return
from ray.data._internal.util import _truncated_repr
from ray.data.block import (
    Block,
    BlockAccessor,
    CallableClass,
    DataBatch,
    UserDefinedFunction,
)
from ray.data.context import DataContext


def plan_udf_map_op(
    op: AbstractUDFMap, input_physical_dag: PhysicalOperator
) -> MapOperator:
    """Get the corresponding physical operators DAG for AbstractUDFMap operators.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """

    compute = get_compute(op._compute)
    fn, init_fn = _parse_op_fn(op)

    if isinstance(op, MapBatches):
        transform_fn = _generate_transform_fn_for_map_batches(fn)
        map_transformer = _create_map_transformer_for_map_batches_op(
            transform_fn,
            op._batch_size,
            op._batch_format,
            op._zero_copy_batch,
            init_fn,
        )
    else:
        if isinstance(op, MapRows):
            transform_fn = _generate_transform_fn_for_map_rows(fn)
        elif isinstance(op, FlatMap):
            transform_fn = _generate_transform_fn_for_flat_map(fn)
        elif isinstance(op, Filter):
            transform_fn = _generate_transform_fn_for_filter(fn)
        else:
            raise ValueError(f"Found unknown logical operator during planning: {op}")

        map_transformer = _create_map_transformer_for_row_based_map_op(
            transform_fn, init_fn
        )

    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        name=op.name,
        target_max_block_size=None,
        compute_strategy=compute,
        min_rows_per_bundle=op._min_rows_per_block,
        ray_remote_args=op._ray_remote_args,
    )


def _parse_op_fn(op: AbstractUDFMap):
    # Note, it's important to define these standalone variables.
    # So the parsed functions won't need to caputure the entire operator, which may not
    # be serializable.
    op_fn = op._fn
    fn_args = op._fn_args or ()
    fn_kwargs = op._fn_kwargs or {}

    if isinstance(op._fn, CallableClass):
        fn_constructor_args = op._fn_constructor_args or ()
        fn_constructor_kwargs = op._fn_constructor_kwargs or {}

        op_fn = make_callable_class_concurrent(op_fn)

        def fn(item: Any) -> Any:
            assert ray.data._cached_fn is not None
            assert ray.data._cached_cls == op_fn
            return ray.data._cached_fn(item, *fn_args, **fn_kwargs)

        def init_fn():
            if ray.data._cached_fn is None:
                ray.data._cached_cls = op_fn
                ray.data._cached_fn = op_fn(
                    *fn_constructor_args, **fn_constructor_kwargs
                )

    else:

        def fn(item: Any) -> Any:
            return op_fn(item, *fn_args, **fn_kwargs)

        def init_fn():
            pass

    return fn, init_fn


# Following are util functions for converting UDFs to `MapTransformCallable`s.


def _validate_batch_output(batch: Block) -> None:
    if not isinstance(
        batch,
        (
            list,
            pa.Table,
            np.ndarray,
            collections.abc.Mapping,
            pd.core.frame.DataFrame,
        ),
    ):
        raise ValueError(
            "The `fn` you passed to `map_batches` returned a value of type "
            f"{type(batch)}. This isn't allowed -- `map_batches` expects "
            "`fn` to return a `pandas.DataFrame`, `pyarrow.Table`, "
            "`numpy.ndarray`, `list`, or `dict[str, numpy.ndarray]`."
        )

    if isinstance(batch, list):
        raise ValueError(
            f"Error validating {_truncated_repr(batch)}: "
            "Returning a list of objects from `map_batches` is not "
            "allowed in Ray 2.5. To return Python objects, "
            "wrap them in a named dict field, e.g., "
            "return `{'results': objects}` instead of just `objects`."
        )

    if isinstance(batch, collections.abc.Mapping):
        for key, value in list(batch.items()):
            if not is_valid_udf_return(value):
                raise ValueError(
                    f"Error validating {_truncated_repr(batch)}: "
                    "The `fn` you passed to `map_batches` returned a "
                    f"`dict`. `map_batches` expects all `dict` values "
                    f"to be `list` or `np.ndarray` type, but the value "
                    f"corresponding to key {key!r} is of type "
                    f"{type(value)}. To fix this issue, convert "
                    f"the {type(value)} to a `np.ndarray`."
                )


def _generate_transform_fn_for_map_batches(
    fn: UserDefinedFunction,
) -> MapTransformCallable[DataBatch, DataBatch]:
    def transform_fn(
        batches: Iterable[DataBatch], _: TaskContext
    ) -> Iterable[DataBatch]:
        for batch in batches:
            try:
                if (
                    not isinstance(batch, collections.abc.Mapping)
                    and BlockAccessor.for_block(batch).num_rows() == 0
                ):
                    # For empty input blocks, we directly ouptut them without
                    # calling the UDF.
                    # TODO(hchen): This workaround is because some all-to-all
                    # operators output empty blocks with no schema.
                    res = [batch]
                else:
                    res = fn(batch)
                    if not isinstance(res, GeneratorType):
                        res = [res]
            except ValueError as e:
                read_only_msgs = [
                    "assignment destination is read-only",
                    "buffer source array is read-only",
                ]
                err_msg = str(e)
                if any(msg in err_msg for msg in read_only_msgs):
                    raise ValueError(
                        f"Batch mapper function {fn.__name__} tried to mutate a "
                        "zero-copy read-only batch. To be able to mutate the "
                        "batch, pass zero_copy_batch=False to map_batches(); "
                        "this will create a writable copy of the batch before "
                        "giving it to fn. To elide this copy, modify your mapper "
                        "function so it doesn't try to mutate its input."
                    ) from e
                else:
                    raise e from None
            else:
                for out_batch in res:
                    _validate_batch_output(out_batch)
                    yield out_batch

    return transform_fn


def _validate_row_output(item):
    if not isinstance(item, collections.abc.Mapping):
        raise ValueError(
            f"Error validating {_truncated_repr(item)}: "
            "Standalone Python objects are not "
            "allowed in Ray 2.5. To return Python objects from map(), "
            "wrap them in a dict, e.g., "
            "return `{'item': item}` instead of just `item`."
        )


def _generate_transform_fn_for_map_rows(
    fn: UserDefinedFunction,
) -> MapTransformCallable[Row, Row]:
    def transform_fn(rows: Iterable[Row], _: TaskContext) -> Iterable[Row]:
        for row in rows:
            out_row = fn(row)
            _validate_row_output(out_row)
            yield out_row

    return transform_fn


def _generate_transform_fn_for_flat_map(
    fn: UserDefinedFunction,
) -> MapTransformCallable[Row, Row]:
    def transform_fn(rows: Iterable[Row], _: TaskContext) -> Iterable[Row]:
        for row in rows:
            for out_row in fn(row):
                _validate_row_output(out_row)
                yield out_row

    return transform_fn


def _generate_transform_fn_for_filter(
    fn: UserDefinedFunction,
) -> MapTransformCallable[Row, Row]:
    def transform_fn(rows: Iterable[Row], _: TaskContext) -> Iterable[Row]:
        for row in rows:
            if fn(row):
                yield row

    return transform_fn


# Following are util functions for creating `MapTransformer`s.


def _create_map_transformer_for_map_batches_op(
    batch_fn: MapTransformCallable[DataBatch, DataBatch],
    batch_size: Optional[int] = None,
    batch_format: str = "default",
    zero_copy_batch: bool = False,
    init_fn: Optional[Callable[[], None]] = None,
) -> MapTransformer:
    """Create a MapTransformer for a map_batches operator."""
    transform_fns = [
        # Convert input blocks to batches.
        BlocksToBatchesMapTransformFn(
            batch_size=batch_size,
            batch_format=batch_format,
            zero_copy_batch=zero_copy_batch,
        ),
        # Apply the UDF.
        BatchMapTransformFn(batch_fn),
        # Convert output batches to blocks.
        BuildOutputBlocksMapTransformFn.for_batches(),
    ]
    return MapTransformer(transform_fns, init_fn)


def _create_map_transformer_for_row_based_map_op(
    row_fn: MapTransformCallable[Row, Row],
    init_fn: Optional[Callable[[], None]] = None,
) -> MapTransformer:
    """Create a MapTransformer for a row-based map operator
    (e.g. map, flat_map, filter)."""
    transform_fns = [
        # Convert input blocks to rows.
        BlocksToRowsMapTransformFn.instance(),
        # Apply the UDF.
        RowMapTransformFn(row_fn),
        # Convert output rows to blocks.
        BuildOutputBlocksMapTransformFn.for_rows(),
    ]
    return MapTransformer(transform_fns, init_fn=init_fn)


# Following are util functions for the legacy code path.


def generate_map_rows_fn(
    target_max_block_size: int,
) -> (Callable[[Iterator[Block], TaskContext, UserDefinedFunction], Iterator[Block]]):
    """Generate function to apply the UDF to each record of blocks."""
    context = DataContext.get_current()

    def fn(
        blocks: Iterator[Block],
        ctx: TaskContext,
        row_fn: UserDefinedFunction,
    ) -> Iterator[Block]:
        DataContext._set_current(context)
        transform_fn = _generate_transform_fn_for_map_rows(row_fn)
        map_transformer = _create_map_transformer_for_row_based_map_op(transform_fn)
        map_transformer.set_target_max_block_size(target_max_block_size)
        yield from map_transformer.apply_transform(blocks, ctx)

    return fn


def generate_flat_map_fn(
    target_max_block_size: int,
) -> (Callable[[Iterator[Block], TaskContext, UserDefinedFunction], Iterator[Block]]):
    """Generate function to apply the UDF to each record of blocks,
    and then flatten results.
    """

    context = DataContext.get_current()

    def fn(
        blocks: Iterator[Block],
        ctx: TaskContext,
        row_fn: UserDefinedFunction,
    ) -> Iterator[Block]:
        DataContext._set_current(context)
        transform_fn = _generate_transform_fn_for_flat_map(row_fn)
        map_transformer = _create_map_transformer_for_row_based_map_op(transform_fn)
        map_transformer.set_target_max_block_size(target_max_block_size)
        yield from map_transformer.apply_transform(blocks, ctx)

    return fn


def generate_filter_fn(
    target_max_block_size: int,
) -> (Callable[[Iterator[Block], TaskContext, UserDefinedFunction], Iterator[Block]]):
    """Generate function to apply the UDF to each record of blocks,
    and filter out records that do not satisfy the given predicate.
    """

    context = DataContext.get_current()

    def fn(
        blocks: Iterator[Block],
        ctx: TaskContext,
        row_fn: UserDefinedFunction,
    ) -> Iterator[Block]:
        DataContext._set_current(context)
        transform_fn = _generate_transform_fn_for_filter(row_fn)
        map_transformer = _create_map_transformer_for_row_based_map_op(transform_fn)
        map_transformer.set_target_max_block_size(target_max_block_size)
        yield from map_transformer.apply_transform(blocks, ctx)

    return fn


def generate_map_batches_fn(
    target_max_block_size: int,
    batch_size: Optional[int] = None,
    batch_format: str = "default",
    zero_copy_batch: bool = False,
) -> Callable[[Iterator[Block], TaskContext, UserDefinedFunction], Iterator[Block]]:
    """Generate function to apply the batch UDF to blocks."""
    context = DataContext.get_current()

    def fn(
        blocks: Iterable[Block],
        ctx: TaskContext,
        batch_fn: UserDefinedFunction,
        *fn_args,
        **fn_kwargs,
    ) -> Iterator[Block]:
        DataContext._set_current(context)

        def _batch_fn(batch):
            return batch_fn(batch, *fn_args, **fn_kwargs)

        transform_fn = _generate_transform_fn_for_map_batches(_batch_fn)
        map_transformer = _create_map_transformer_for_map_batches_op(
            transform_fn,
            batch_size,
            batch_format,
            zero_copy_batch,
        )
        map_transformer.set_target_max_block_size(target_max_block_size)
        yield from map_transformer.apply_transform(blocks, ctx)

    return fn
