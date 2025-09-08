import asyncio
import collections
import inspect
import logging
import queue
from threading import Thread
from types import GeneratorType
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
)

import numpy as np
import pandas as pd
import pyarrow as pa

import ray
from ray._common.utils import get_or_create_event_loop
from ray._private.ray_constants import env_integer

logger = logging.getLogger(__name__)
from ray.data._expression_evaluator import eval_expr
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
    Check,
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
    CallableClass,
    DataBatch,
    UserDefinedFunction,
)
from ray.data.context import DataContext
from ray.data.exceptions import UserCodeException
from ray.util.rpdb import _is_ray_debugger_post_mortem_enabled

logger = logging.getLogger(__name__)


# Controls default max-concurrency setting for async row-based UDFs
DEFAULT_ASYNC_ROW_UDF_MAX_CONCURRENCY = env_integer(
    "RAY_DATA_DEFAULT_ASYNC_ROW_UDF_MAX_CONCURRENCY", 16
)

# Controls default max-concurrency setting for async batch-based UDFs
DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY = env_integer(
    "RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY", 4
)


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
    exprs = op.exprs

    def fn(block: Block) -> Block:
        try:
            block_accessor = BlockAccessor.for_block(block)
            if not block_accessor.num_rows():
                return block

            # 1. evaluate / add expressions
            if exprs:
                block_accessor = BlockAccessor.for_block(block)
                # Add/update with expression results
                result_block = block
                for name, expr in exprs.items():
                    result = eval_expr(expr, result_block)
                    result_block_accessor = BlockAccessor.for_block(result_block)
                    result_block = result_block_accessor.upsert_column(name, result)

                block = result_block

            # 2. (optional) column projection
            if columns:
                block = BlockAccessor.for_block(block).select(columns)

            # 3. (optional) rename
            if columns_rename:
                block = block.rename_columns(
                    [columns_rename.get(col, col) for col in block.schema.names]
                )

            return block
        except Exception as e:
            _try_wrap_udf_exception(e)

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
    # Disable fusion for streaming repartition with the downstream op.
    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name=op.name,
        compute_strategy=compute,
        ray_remote_args=op._ray_remote_args,
        ray_remote_args_fn=op._ray_remote_args_fn,
        supports_fusion=False,
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
                _try_wrap_udf_exception(e)

        transform_fn = _generate_transform_fn_for_map_batches(filter_batch_fn)
        map_transformer = _create_map_transformer_for_map_batches_op(
            transform_fn,
            batch_size=None,
            batch_format="pyarrow",
            zero_copy_batch=True,
        )
    else:
        udf_is_callable_class = isinstance(op._fn, CallableClass)
        filter_fn, init_fn = _get_udf(
            op._fn,
            op._fn_args,
            op._fn_kwargs,
            op._fn_constructor_args if udf_is_callable_class else None,
            op._fn_constructor_kwargs if udf_is_callable_class else None,
        )
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


def plan_check_op(
    op: Check,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    """Plan the Check logical operator into a physical MapOperator.
    
    This creates a MapOperator that performs data quality checks and handles
    violations according to the specified policy. Quarantine data is collected
    and deferred for writing at the end of dataset execution.
    """
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    expression = op._check_expr
    compute = get_compute(op._compute)
    
    if expression is not None:
        # Use Arrow expression for optimal performance
        def check_batch_fn(block: "pa.Table") -> "pa.Table":
            try:
                return _process_check_batch_with_expression(
                    block, 
                    expression, 
                    op._on_violation,
                    op._max_failures,
                    op._quarantine_path,
                    op._quarantine_format,
                    op._metadata_path
                )
            except Exception as e:
                _try_wrap_udf_exception(e)

        transform_fn = _generate_transform_fn_for_map_batches(check_batch_fn)
        map_transformer = _create_map_transformer_for_map_batches_op(
            transform_fn,
            batch_size=None,
            batch_format="pyarrow",
            zero_copy_batch=True,
        )
    else:
        # Use callable function
        udf_is_callable_class = isinstance(op._fn, CallableClass)
        check_fn, init_fn = _get_udf(
            op._fn,
            op._fn_args,
            op._fn_kwargs,
            op._fn_constructor_args if udf_is_callable_class else None,
            op._fn_constructor_kwargs if udf_is_callable_class else None,
        )
        transform_fn = _generate_transform_fn_for_check(
            check_fn,
            op._on_violation,
            op._max_failures,
            op._quarantine_path,
            op._quarantine_format,
            op._metadata_path
        )
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
    udf_is_callable_class = isinstance(op._fn, CallableClass)
    fn, init_fn = _get_udf(
        op._fn,
        op._fn_args,
        op._fn_kwargs,
        op._fn_constructor_args if udf_is_callable_class else None,
        op._fn_constructor_kwargs if udf_is_callable_class else None,
    )

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
            elif isinstance(op, Check):
                # Check should be handled by plan_check_op, not here
                raise ValueError(f"Check operator should be handled by plan_check_op, not plan_udf_map_op")
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


def _get_udf(
    op_fn: Callable,
    op_fn_args: Tuple[Any, ...],
    op_fn_kwargs: Dict[str, Any],
    op_fn_constructor_args: Optional[Tuple[Any, ...]],
    op_fn_constructor_kwargs: Optional[Dict[str, Any]],
):
    # Note, it's important to define these standalone variables.
    # So the parsed functions won't need to capture the entire operator, which may not
    # be serializable.
    udf = op_fn
    fn_args = op_fn_args or ()
    fn_kwargs = op_fn_kwargs or {}

    if isinstance(udf, CallableClass):
        fn_constructor_args = op_fn_constructor_args or ()
        fn_constructor_kwargs = op_fn_constructor_kwargs or {}

        is_async_udf = _is_async_udf(udf.__call__)

        if not is_async_udf:
            # TODO(ak) this constrains concurrency for user UDFs to run in a single
            #          thread irrespective of max_concurrency. Remove
            udf = make_callable_class_concurrent(udf)

        def init_fn():
            if ray.data._map_actor_context is None:
                ray.data._map_actor_context = _MapActorContext(
                    udf_map_cls=udf,
                    udf_map_fn=udf(
                        *fn_constructor_args,
                        **fn_constructor_kwargs,
                    ),
                    is_async=is_async_udf,
                )

        if inspect.iscoroutinefunction(udf.__call__):

            async def _wrapped_udf_map_fn(item: Any) -> Any:
                assert ray.data._map_actor_context is not None
                assert ray.data._map_actor_context.is_async

                try:
                    return await ray.data._map_actor_context.udf_map_fn(
                        item,
                        *fn_args,
                        **fn_kwargs,
                    )
                except Exception as e:
                    _try_wrap_udf_exception(e)

        elif inspect.isasyncgenfunction(udf.__call__):

            async def _wrapped_udf_map_fn(item: Any) -> Any:
                assert ray.data._map_actor_context is not None
                assert ray.data._map_actor_context.is_async

                try:
                    gen = ray.data._map_actor_context.udf_map_fn(
                        item,
                        *fn_args,
                        **fn_kwargs,
                    )

                    async for res in gen:
                        yield res
                except Exception as e:
                    _try_wrap_udf_exception(e, item)

        else:
            assert isinstance(
                udf.__call__, Callable
            ), f"Expected Callable, got {udf.__call__} ({type(udf.__call__)})"

            def _wrapped_udf_map_fn(item: Any) -> Any:
                assert ray.data._map_actor_context is not None
                assert not ray.data._map_actor_context.is_async
                try:
                    return ray.data._map_actor_context.udf_map_fn(
                        item,
                        *fn_args,
                        **fn_kwargs,
                    )
                except Exception as e:
                    _try_wrap_udf_exception(e)

    else:

        def _wrapped_udf_map_fn(item: Any) -> Any:
            try:
                return udf(item, *fn_args, **fn_kwargs)
            except Exception as e:
                _try_wrap_udf_exception(e)

        def init_fn():
            pass

    return _wrapped_udf_map_fn, init_fn


def _try_wrap_udf_exception(e: Exception, item: Any = None):
    """If the Ray Debugger is enabled, keep the full stack trace unmodified
    so that the debugger can stop at the initial unhandled exception.
    Otherwise, clear the stack trace to omit noisy internal code path."""
    ctx = ray.data.DataContext.get_current()
    if _is_ray_debugger_post_mortem_enabled() or ctx.raise_original_map_exception:
        raise e
    else:
        raise UserCodeException("UDF failed to process a data block.") from e


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

    if _is_async_udf(fn):
        transform_fn = _generate_transform_fn_for_async_map(
            fn,
            _validate_batch_output,
            max_concurrency=DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY,
        )

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
                        # For empty input blocks, we directly output them without
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


def _is_async_udf(fn: UserDefinedFunction) -> bool:
    return inspect.iscoroutinefunction(fn) or inspect.isasyncgenfunction(fn)


def _validate_row_output(item):
    if not isinstance(item, collections.abc.Mapping):
        raise ValueError(
            f"Error validating {_truncated_repr(item)}: "
            "Standalone Python objects are not "
            "allowed in Ray >= 2.5. To return Python objects from map(), "
            "wrap them in a dict, e.g., "
            "return `{'item': item}` instead of just `item`."
        )


def _generate_transform_fn_for_map_rows(
    fn: UserDefinedFunction,
) -> MapTransformCallable[Row, Row]:

    if _is_async_udf(fn):
        transform_fn = _generate_transform_fn_for_async_map(
            fn,
            _validate_row_output,
            # NOTE: UDF concurrency is limited
            max_concurrency=DEFAULT_ASYNC_ROW_UDF_MAX_CONCURRENCY,
        )

    else:

        def transform_fn(rows: Iterable[Row], _: TaskContext) -> Iterable[Row]:
            for row in rows:
                out_row = fn(row)
                _validate_row_output(out_row)
                yield out_row

    return transform_fn


def _generate_transform_fn_for_flat_map(
    fn: UserDefinedFunction,
) -> MapTransformCallable[Row, Iterable[Row]]:
    if _is_async_udf(fn):
        # UDF is a callable class with async generator `__call__` method.
        transform_fn = _generate_transform_fn_for_async_map(
            fn,
            _validate_row_output,
            max_concurrency=DEFAULT_ASYNC_ROW_UDF_MAX_CONCURRENCY,
            is_flat_map=True,
        )

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


_SENTINEL = object()

T = TypeVar("T")
U = TypeVar("U")


def _generate_transform_fn_for_async_map(
    fn: UserDefinedFunction,
    validate_fn: Callable,
    *,
    max_concurrency: int,
    is_flat_map: bool = False,
) -> MapTransformCallable:
    assert max_concurrency > 0, "Max concurrency must be positive"

    if inspect.isasyncgenfunction(fn):

        async def _apply_udf(item: T) -> List[U]:
            gen = fn(item)
            # NOTE: Async generator is unrolled inside the task to maintain
            #       requested concurrency level (`max_concurrent_batches`)
            return [out async for out in gen]

    elif inspect.iscoroutinefunction(fn):

        async def _apply_udf(item: T) -> List[U]:
            res = await fn(item)
            return res if is_flat_map else [res]

    else:
        raise ValueError(f"Expected a coroutine function, got {fn}")

    # Goals of the algorithm applying async UDF application to the provided iterator
    # are following:
    #
    #   - No more than `max_concurrency` async tasks are running
    #     at any given moment
    #   - Slow consumption from the output queue should result in
    #     the processing to get back-pressured (so that output queue
    #     doesn't grow unbounded)
    #   - Order of the items (rows/batches) produced by this method
    #     *must be* deterministic (though is not guaranteed to be specified
    #     if max_concurrency > 1)
    #
    # To achieve that, algorithm applying async UDF to elements of the provided sequence
    # is structured like following:
    #
    #   - Task scheduling and subsequent results re-ordering are performed as
    #     different stages (inside `_schedule` and `_report` methods respectively)
    #
    #   - Scheduling stage aim to schedule and run no more than `max_concurrency` tasks
    #     at any given moment
    #
    #   - Once task completes it's added into task completion queue for its results to be
    #     subsequently reported with deterministic ordering). Task completion queue is
    #     capped at `maxsize=max_concurrency` elements to make sure scheduling stage is
    #     throttled (and task completion queue isn't growing unbounded) in case when
    #     reporting stage isn't able to keep up.
    #
    #   - Reporting stage dequeues completed tasks from completion queue, reorders
    #     them (to *always* produce deterministic ordering) and adds its results into
    #     output queue.
    #
    #   - Output queue is capped at `maxsize=max_concurrency` elements to make sure that
    #     reporting stage is throttled (and output queue doesn't grow unbounded) in case
    #     when consumer (Ray task itself) isn't able to keep up
    #
    async def _execute_transform(it: Iterator[T], output_queue: queue.Queue) -> None:
        loop = asyncio.get_running_loop()

        # NOTE: Individual tasks could complete in arbitrary order.
        #       To make sure that the ordering produced by this transformation
        #       is deterministic we utilize subsequent reordering stage to
        #       to keep the output ordering the same as that one of the input
        #       iterator.
        completed_tasks_queue = asyncio.Queue(maxsize=max_concurrency)
        # NOTE: This method is nested to support Python 3.9 where we only can
        #       init `asyncio.Queue` inside the async function
        async def _reorder() -> None:
            completed_task_map: Dict[int, asyncio.Task] = dict()
            next_idx = 0
            completed_scheduling = False

            try:
                while not completed_scheduling:
                    task, idx = await completed_tasks_queue.get()

                    if isinstance(task, Exception):
                        raise task
                    elif task is _SENTINEL:
                        completed_scheduling = True
                    else:
                        completed_task_map[idx] = task

                    while next_idx in completed_task_map:
                        next_task = completed_task_map.pop(next_idx)

                        # NOTE: Once output queue fills up, this will block
                        #       therefore serving as back-pressure for scheduling tasks
                        #       preventing it from scheduling new tasks.
                        # NOTE: This will block the whole event-loop not just this task
                        output_queue.put(await next_task)

                        next_idx += 1

                assert (
                    len(completed_task_map) == 0
                ), f"{next_idx=}, {completed_task_map.keys()=}"
                sentinel = _SENTINEL

            except BaseException as e:
                sentinel = e
            finally:
                output_queue.put(sentinel)

        # NOTE: Reordering is an async process
        asyncio.create_task(_reorder())

        cur_task_map: Dict[asyncio.Task, int] = dict()
        consumed = False

        sentinel = _SENTINEL
        enumerated_it = enumerate(it)

        try:
            while True:
                while len(cur_task_map) < max_concurrency and not consumed:
                    try:
                        idx, item = next(enumerated_it)
                        # Launch async task while keeping track of its
                        # index in the enumerated sequence
                        task = loop.create_task(_apply_udf(item))
                        cur_task_map[task] = idx
                    except StopIteration:
                        consumed = True
                        break

                # Check if any running tasks remaining
                if not cur_task_map:
                    break

                done, pending = await asyncio.wait(
                    cur_task_map.keys(), return_when=asyncio.FIRST_COMPLETED
                )

                for task in done:
                    # Report completed tasks along w/ its corresponding
                    # index in the input sequence
                    #
                    # NOTE: Once completed tasks queue fills up, this will block
                    #       therefore serving as back-pressure for scheduling tasks
                    #       preventing it from scheduling new tasks
                    await completed_tasks_queue.put((task, cur_task_map[task]))

                    cur_task_map.pop(task)

        except BaseException as e:
            for cur_task in cur_task_map:
                if not cur_task.done():
                    cur_task.cancel()

            sentinel = e
        finally:
            assert len(cur_task_map) == 0, f"{cur_task_map}"
            await completed_tasks_queue.put((sentinel, None))

    def _transform(batch_iter: Iterable[T], task_context: TaskContext) -> Iterable[U]:
        output_queue = queue.Queue(maxsize=max_concurrency)

        loop = ray.data._map_actor_context.udf_map_asyncio_loop

        asyncio.run_coroutine_threadsafe(
            _execute_transform(iter(batch_iter), output_queue), loop
        )

        while True:
            items = output_queue.get()
            if items is _SENTINEL:
                break
            elif isinstance(items, Exception):
                raise items
            else:
                # NOTE: Sequences from individual UDFs are combined into a single
                #       sequence here, as compared to letting individual UDFs to
                #       add into the output queue to guarantee *deterministic* ordering
                #       (necessary for Ray Data to be able to guarantee task retries
                #       producing the same results)
                for item in items:
                    validate_fn(item)
                    yield item

    return _transform


# Check-specific functions for data quality validation

def _generate_transform_fn_for_check(
    fn: UserDefinedFunction,
    on_violation: str,
    max_failures: Optional[int],
    quarantine_path: Optional[str],
    quarantine_format: str,
    metadata_path: Optional[str],
) -> MapTransformCallable[Row, Row]:
    """Generate transform function for row-based check operations."""
    
    def transform_fn(rows: Iterable[Row], ctx: TaskContext) -> Iterable[Row]:
        violation_count = 0
        quarantine_rows = []
        processed_count = 0
        start_time = time.time()
        
        # Get or create violation tracker
        from ray.data._internal.data_quality_utils import _get_violation_tracker, DataQualityError, get_check_metrics
        violation_tracker = _get_violation_tracker(ctx, getattr(fn, '__name__', 'anonymous_check'))
        metrics = get_check_metrics(getattr(fn, '__name__', 'anonymous_check'))
        
        for row_idx, row in enumerate(rows):
            processed_count += 1
            try:
                # Perform schema validation first
                validated_row = _validate_row_schema(row, ctx)
                
                # Apply the check function
                is_valid = fn(validated_row)
                
                if not is_valid:
                    violation_count += 1
                    violation_tracker.add_violation()
                    
                    # Check global max failures early
                    if max_failures is not None and violation_tracker.get_total_violations() >= max_failures:
                        _handle_quarantine_data(quarantine_rows, quarantine_path, quarantine_format)
                        raise ValueError(f"Maximum failures ({max_failures}) exceeded globally")
                    
                    # Handle violation based on policy
                    if on_violation == "fail":
                        violation_details = {
                            "row_index": row_idx,
                            "row_data": row,
                            "check_name": getattr(fn, '__name__', 'anonymous_check'),
                            "violation_reason": "check_failed"
                        }
                        from ray.data._internal.data_quality_utils import DataQualityError
                        raise DataQualityError(f"Data quality check failed for row {row_idx}", violation_details)
                    elif on_violation == "warn":
                        logger.warning(
                            f"Data quality violation at row {row_idx} "
                            f"(total violations: {violation_tracker.get_total_violations()}): {row}"
                        )
                        yield validated_row
                    elif on_violation == "drop":
                        continue  # Skip this row
                    elif on_violation == "quarantine":
                        # Add comprehensive metadata to quarantined row
                        quarantine_row = {
                            **validated_row,
                            "_dq_violation_reason": "check_failed",
                            "_dq_violation_timestamp": time.time(),
                            "_dq_row_index": row_idx,
                            "_dq_batch_id": getattr(ctx, 'batch_id', 'unknown'),
                            "_dq_check_name": getattr(fn, '__name__', 'anonymous_check'),
                            "_dq_worker_id": getattr(ctx, 'worker_id', 'unknown')
                        }
                        quarantine_rows.append(quarantine_row)
                        continue  # Don't yield to main output
                else:
                    yield validated_row
                    
            except DataQualityError:
                # Re-raise data quality errors
                raise
            except Exception as e:
                violation_count += 1
                violation_tracker.add_violation()
                
                if on_violation == "fail":
                    raise DataQualityError(f"Processing error at row {row_idx}: {str(e)}", {
                        "row_index": row_idx,
                        "row_data": row,
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    })
                elif on_violation in ["warn", "quarantine"]:
                    # Handle schema or processing errors
                    error_row = {
                        **row,
                        "_dq_violation_reason": f"processing_error: {str(e)}",
                        "_dq_violation_timestamp": time.time(),
                        "_dq_row_index": row_idx,
                        "_dq_error_type": type(e).__name__,
                        "_dq_batch_id": getattr(ctx, 'batch_id', 'unknown')
                    }
                    if on_violation == "warn":
                        logger.warning(f"Processing error at row {row_idx}: {e}")
                        yield row
                    else:  # quarantine
                        quarantine_rows.append(error_row)
                
                # Check global max failures
                if max_failures is not None and violation_tracker.get_total_violations() >= max_failures:
                    _handle_quarantine_data(quarantine_rows, quarantine_path, quarantine_format)
                    raise ValueError(f"Maximum failures ({max_failures}) exceeded globally")
        
        # Record batch statistics
        end_time = time.time()
        batch_stats = {
            "processed_rows": processed_count,
            "violations": violation_count,
            "quarantined_rows": len(quarantine_rows),
            "processing_time": end_time - start_time,
            "violation_rate": violation_count / processed_count if processed_count > 0 else 0
        }
        
        # Update metrics
        metrics.update_batch_stats(batch_stats)
        
        # Store batch statistics in context for later aggregation
        if hasattr(ctx, 'batch_stats'):
            ctx.batch_stats.append(batch_stats)
        
        # Handle any remaining quarantine data at end of batch
        if quarantine_rows and on_violation == "quarantine":
            check_id = getattr(fn, '__name__', 'anonymous_check')
            _handle_quarantine_data(quarantine_rows, quarantine_path, quarantine_format, check_id)
        
        # Write metadata if path provided
        if metadata_path and violation_count > 0:
            from ray.data._internal.data_quality_utils import _write_batch_metadata
            _write_batch_metadata(metadata_path, batch_stats, ctx)

    return transform_fn


def _process_check_batch_with_expression(
    block: "pa.Table",
    expression: "pa.dataset.Expression", 
    on_violation: str,
    max_failures: Optional[int],
    quarantine_path: Optional[str],
    quarantine_format: str,
    metadata_path: Optional[str]
) -> "pa.Table":
    """Process a batch of data using Arrow expression for data quality checks."""
    import pyarrow as pa
    import pyarrow.compute as pc
    
    # Perform schema validation first
    validated_block = _validate_block_schema(block)
    
    try:
        # Apply the check expression
        mask = expression.evaluate(validated_block)
        valid_mask = pc.is_true(mask)
        invalid_mask = pc.invert(valid_mask)
        
        # Count violations
        violation_count = pc.sum(pc.cast(invalid_mask, pa.int64())).as_py()
        
        if violation_count > 0:
            if on_violation == "fail":
                raise ValueError(f"Data quality check failed for {violation_count} rows")
            elif on_violation == "warn":
                logger.warning(f"Data quality violations found in {violation_count} rows")
                return validated_block
            elif on_violation == "drop":
                return validated_block.filter(valid_mask)
            elif on_violation == "quarantine":
                # Extract violating rows for quarantine
                invalid_rows = validated_block.filter(invalid_mask)
                
                # Add metadata columns
                metadata_columns = {
                    "_dq_violation_reason": pa.array(["expression_failed"] * len(invalid_rows)),
                    "_dq_violation_timestamp": pa.array([time.time()] * len(invalid_rows)),
                    "_dq_check_expression": pa.array([str(expression)] * len(invalid_rows))
                }
                
                # Combine original data with metadata
                quarantine_block = invalid_rows
                for col_name, col_data in metadata_columns.items():
                    quarantine_block = quarantine_block.append_column(col_name, col_data)
                
                # Store quarantine data for deferred writing with check ID
                check_id = f"expression_check_{hash(str(expression)) % 10000}"
                _handle_quarantine_block(quarantine_block, quarantine_path, quarantine_format, check_id)
                
                # Return only valid rows
                return validated_block.filter(valid_mask)
        
        # Check max failures
        if max_failures is not None and violation_count >= max_failures:
            raise ValueError(f"Maximum failures ({max_failures}) exceeded")
        
        return validated_block
        
    except Exception as e:
        if on_violation == "fail":
            raise
        logger.warning(f"Error processing batch with expression: {e}")
        return validated_block


def _validate_row_schema(row: Row, ctx: TaskContext) -> Row:
    """Validate and potentially fix row schema issues."""
    # Get expected schema from context if available
    expected_schema = getattr(ctx, 'expected_schema', None)
    
    if expected_schema is None:
        return row
    
    validated_row = {}
    schema_violations = []
    
    # Check for missing columns
    for field in expected_schema:
        field_name = field.name
        if field_name not in row:
            if field.nullable:
                validated_row[field_name] = None
            else:
                schema_violations.append(f"Missing required field: {field_name}")
        else:
            # Type validation and coercion
            value = row[field_name]
            try:
                # Attempt type coercion if needed
                validated_row[field_name] = _coerce_value_to_type(value, field.type)
            except (TypeError, ValueError) as e:
                schema_violations.append(f"Type mismatch for field {field_name}: {e}")
                validated_row[field_name] = value  # Keep original value
    
    # Check for extra columns
    for field_name in row:
        if field_name not in [f.name for f in expected_schema]:
            validated_row[field_name] = row[field_name]  # Keep extra columns
    
    # Add schema violation metadata if any issues found
    if schema_violations:
        validated_row["_dq_schema_violations"] = schema_violations
    
    return validated_row


def _validate_block_schema(block: "pa.Table") -> "pa.Table":
    """Validate and potentially fix block schema issues."""
    import pyarrow as pa
    
    # For now, return as-is but this could be extended to handle:
    # - Column type mismatches
    # - Missing columns
    # - Schema evolution
    # - Union of multiple schemas
    
    return block


def _coerce_value_to_type(value: Any, target_type: "pa.DataType") -> Any:
    """Attempt to coerce a value to the target PyArrow type."""
    import pyarrow as pa
    
    if value is None:
        return None
    
    # Handle common type coercions
    if pa.types.is_string(target_type):
        return str(value)
    elif pa.types.is_integer(target_type):
        return int(value)
    elif pa.types.is_floating(target_type):
        return float(value)
    elif pa.types.is_boolean(target_type):
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'on')
        return bool(value)
    else:
        return value


def _handle_quarantine_data(rows: List[Row], quarantine_path: Optional[str], quarantine_format: str, check_id: Optional[str] = None):
    """Handle quarantine data by storing it for deferred writing."""
    if not rows or not quarantine_path:
        return
    
    from ray.data._internal.quarantine_manager import get_quarantine_manager
    
    manager = get_quarantine_manager()
    violation_metadata = {
        "quarantine_reason": "data_quality_violation",
        "timestamp": time.time(),
        "check_id": check_id,
    }
    
    manager.add_quarantine_rows(rows, quarantine_path, quarantine_format, violation_metadata, check_id)


def _handle_quarantine_block(block: "pa.Table", quarantine_path: Optional[str], quarantine_format: str, check_id: Optional[str] = None):
    """Handle quarantine block by storing it for deferred writing."""
    if len(block) == 0 or not quarantine_path:
        return
    
    from ray.data._internal.quarantine_manager import get_quarantine_manager
    
    manager = get_quarantine_manager()
    violation_metadata = {
        "quarantine_reason": "data_quality_violation",
        "timestamp": time.time(),
        "check_id": check_id,
    }
    
    manager.add_quarantine_block(block, quarantine_path, quarantine_format, violation_metadata, check_id)


