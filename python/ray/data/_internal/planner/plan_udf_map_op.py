import asyncio
import collections
import inspect
import logging
import queue
from threading import Thread
from types import GeneratorType
from typing import Any, Callable, Iterable, List, Optional

import numpy as np
import pandas as pd
import pyarrow as pa

import ray
from ray._common.utils import get_or_create_event_loop
from ray.data._internal.compute import get_compute
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BatchMapTransformFn,
    BlockMapTransformFn,
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
    Project,
    StreamingRepartition,
)
from ray.data._internal.numpy_support import _is_valid_column_values
from ray.data._internal.util import _truncated_repr
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockType,
    CallableClass,
    DataBatch,
    UserDefinedFunction,
)
from ray.data.context import DataContext
from ray.data.exceptions import UserCodeException
from ray.util.rpdb import _is_ray_debugger_post_mortem_enabled


logger = logging.getLogger(__name__)


class _MapActorContext:
    def __init__(
        self,
        udf_map_cls: UserDefinedFunction,
        udf_map_fn: Callable[[Any], Any],
        is_async: bool,
    ):
        self.udf_map_cls = udf_map_cls
        self.udf_map_fn = udf_map_fn
        self.is_async = is_async
        self.udf_map_asyncio_loop = None
        self.udf_map_asyncio_thread = None

        if is_async:
            self._init_async()

    def _init_async(self):
        # Only used for callable class with async generator `__call__` method.
        loop = get_or_create_event_loop()

        def run_loop():
            asyncio.set_event_loop(loop)
            loop.run_forever()

        thread = Thread(target=run_loop)
        thread.start()
        self.udf_map_asyncio_loop = loop
        self.udf_map_asyncio_thread = thread


def plan_project_op(
    op: Project,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    columns = op.cols
    columns_rename = op.cols_rename

    def fn(block: Block) -> Block:
        try:
            if BlockAccessor.for_block(block).block_type() == BlockType.PANDAS:
                # TODO (srinathk) PandasBlockAccessor combine method needs to handle
                # None types correctly. Until then, convert to Arrow Table.
                block = BlockAccessor.for_block(block).to_arrow()
            if not BlockAccessor.for_block(block).num_rows():
                return block
            if columns:
                block = BlockAccessor.for_block(block).select(columns)
            if columns_rename:
                block = block.rename_columns(
                    [columns_rename.get(col, col) for col in block.schema.names]
                )
            return block
        except Exception as e:
            _handle_debugger_exception(e, block)

    compute = get_compute(op._compute)
    transform_fn = _generate_transform_fn_for_map_block(fn)
    map_transformer = _create_map_transformer_for_block_based_map_op(
        transform_fn,
    )

    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name=op.name,
        compute_strategy=compute,
        ray_remote_args=op._ray_remote_args,
        ray_remote_args_fn=op._ray_remote_args_fn,
    )


def plan_streaming_repartition_op(
    op: StreamingRepartition,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]
    compute = get_compute(op._compute)
    transform_fn = BuildOutputBlocksMapTransformFn.for_blocks()
    transform_fn.set_target_num_rows_per_block(op.target_num_rows_per_block)
    map_transformer = MapTransformer([transform_fn])
    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name=op.name,
        compute_strategy=compute,
        ray_remote_args=op._ray_remote_args,
        ray_remote_args_fn=op._ray_remote_args_fn,
    )


def plan_filter_op(
    op: Filter,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    expression = op._filter_expr
    compute = get_compute(op._compute)
    if expression is not None:

        def filter_batch_fn(block: "pa.Table") -> "pa.Table":
            try:
                return block.filter(expression)
            except Exception as e:
                _handle_debugger_exception(e, block)

        transform_fn = _generate_transform_fn_for_map_batches(filter_batch_fn)
        map_transformer = _create_map_transformer_for_map_batches_op(
            transform_fn,
            batch_size=None,
            batch_format="pyarrow",
            zero_copy_batch=True,
        )
    else:
        filter_fn, init_fn = _parse_op_fn(op)
        transform_fn = _generate_transform_fn_for_filter(filter_fn)
        map_transformer = _create_map_transformer_for_row_based_map_op(
            transform_fn, init_fn
        )

    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name=op.name,
        compute_strategy=compute,
        ray_remote_args=op._ray_remote_args,
        ray_remote_args_fn=op._ray_remote_args_fn,
    )


def plan_udf_map_op(
    op: AbstractUDFMap,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    """Get the corresponding physical operators DAG for AbstractUDFMap operators.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

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
        else:
            raise ValueError(f"Found unknown logical operator during planning: {op}")

        map_transformer = _create_map_transformer_for_row_based_map_op(
            transform_fn, init_fn
        )

    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name=op.name,
        target_max_block_size=None,
        compute_strategy=compute,
        min_rows_per_bundle=op._min_rows_per_bundled_input,
        ray_remote_args_fn=op._ray_remote_args_fn,
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

        is_async_gen = inspect.isasyncgenfunction(op._fn.__call__)

        # TODO(scottjlee): (1) support non-generator async functions
        # (2) make the map actor async
        if not is_async_gen:
            op_fn = make_callable_class_concurrent(op_fn)

        def init_fn():
            if ray.data._map_actor_context is None:
                ray.data._map_actor_context = _MapActorContext(
                    udf_map_cls=op_fn,
                    udf_map_fn=op_fn(
                        *fn_constructor_args,
                        **fn_constructor_kwargs,
                    ),
                    is_async=is_async_gen,
                )

        if is_async_gen:

            async def fn(item: Any) -> Any:
                assert ray.data._map_actor_context is not None
                assert ray.data._map_actor_context.is_async

                try:
                    return ray.data._map_actor_context.udf_map_fn(
                        item,
                        *fn_args,
                        **fn_kwargs,
                    )
                except Exception as e:
                    _handle_debugger_exception(e, item)

        else:

            def fn(item: Any) -> Any:
                assert ray.data._map_actor_context is not None
                assert not ray.data._map_actor_context.is_async
                try:
                    return ray.data._map_actor_context.udf_map_fn(
                        item,
                        *fn_args,
                        **fn_kwargs,
                    )
                except Exception as e:
                    _handle_debugger_exception(e, item)

    else:

        def fn(item: Any) -> Any:
            try:
                return op_fn(item, *fn_args, **fn_kwargs)
            except Exception as e:
                _handle_debugger_exception(e, item)

        def init_fn():
            pass

    return fn, init_fn


def _handle_debugger_exception(e: Exception, item: Any = None):
    """If the Ray Debugger is enabled, keep the full stack trace unmodified
    so that the debugger can stop at the initial unhandled exception.
    Otherwise, clear the stack trace to omit noisy internal code path."""
    error_message = f"Failed to process the following data block: {item}"

    ctx = ray.data.DataContext.get_current()
    if _is_ray_debugger_post_mortem_enabled() or ctx.raise_original_map_exception:
        logger.error(error_message)
        raise e
    else:
        raise UserCodeException(error_message) from e


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
            dict,
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
            if not _is_valid_column_values(value):
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
    if inspect.iscoroutinefunction(fn):
        # UDF is a callable class with async generator `__call__` method.
        transform_fn = _generate_transform_fn_for_async_map(fn, _validate_batch_output)

    else:

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


def _generate_transform_fn_for_async_map(
    fn: UserDefinedFunction,
    validate_fn,
) -> MapTransformCallable:
    # Generates a transform function for asynchronous mapping of items (either batches or rows)
    # using a user-defined function (UDF). This consolidated function handles both asynchronous
    # batch processing and asynchronous flat mapping (e.g., rows) based on the provided UDF.
    def transform_fn(input_iterable: Iterable, _: TaskContext) -> Iterable:
        # Use a queue to store outputs from async generator calls.
        # We will put output items into this queue from async
        # generators, and in the main event loop, yield them from
        # the queue as they become available.
        output_item_queue = queue.Queue()
        # Sentinel object to signal the end of the async generator.
        sentinel = object()

        async def process_item(item):
            try:
                output_item_iterator = await fn(item)
                # As soon as results become available from the async generator,
                # put them into the result queue so they can be yielded.
                async for output_item in output_item_iterator:
                    output_item_queue.put(output_item)
            except Exception as e:
                output_item_queue.put(
                    e
                )  # Put the exception into the queue to signal an error

        async def process_all_items():
            try:
                loop = ray.data._map_actor_context.udf_map_asyncio_loop
                tasks = [loop.create_task(process_item(x)) for x in input_iterable]

                ctx = ray.data.DataContext.get_current()
                if ctx.execution_options.preserve_order:
                    for task in tasks:
                        await task
                else:
                    for task in asyncio.as_completed(tasks):
                        await task
            finally:
                output_item_queue.put(sentinel)

        # Use the existing event loop to create and run Tasks to process each item
        loop = ray.data._map_actor_context.udf_map_asyncio_loop
        asyncio.run_coroutine_threadsafe(process_all_items(), loop)

        # Yield results as they become available.
        while True:
            # Here, `out_item` is a one-row output item
            # from the async generator, corresponding to a
            # single row from the input item.
            out_item = output_item_queue.get()
            if out_item is sentinel:
                # Break out of the loop when the sentinel is received.
                break
            if isinstance(out_item, Exception):
                raise out_item
            validate_fn(out_item)
            yield out_item

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
    if inspect.iscoroutinefunction(fn):
        # UDF is a callable class with async generator `__call__` method.
        transform_fn = _generate_transform_fn_for_async_map(fn, _validate_row_output)

    else:

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


def _generate_transform_fn_for_map_block(
    fn: UserDefinedFunction,
) -> MapTransformCallable[Block, Block]:
    def transform_fn(blocks: Iterable[Block], _: TaskContext) -> Iterable[Block]:
        for block in blocks:
            out_block = fn(block)
            yield out_block

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
        BatchMapTransformFn(batch_fn, is_udf=True),
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
        RowMapTransformFn(row_fn, is_udf=True),
        # Convert output rows to blocks.
        BuildOutputBlocksMapTransformFn.for_rows(),
    ]
    return MapTransformer(transform_fns, init_fn=init_fn)


def _create_map_transformer_for_block_based_map_op(
    block_fn: MapTransformCallable[Block, Block],
    init_fn: Optional[Callable[[], None]] = None,
) -> MapTransformer:
    """Create a MapTransformer for a block-based map operator."""
    transform_fns = [
        # Apply the UDF.
        BlockMapTransformFn(block_fn),
        BuildOutputBlocksMapTransformFn.for_blocks(),
    ]
    return MapTransformer(transform_fns, init_fn=init_fn)
