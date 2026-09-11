import asyncio
import collections
import inspect
import logging
import queue
from dataclasses import dataclass
from threading import Thread
from types import GeneratorType
from typing import (
    TYPE_CHECKING,
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

if TYPE_CHECKING:
    from ray.data.expressions import _CallableClassSpec

import numpy as np
import pandas as pd
import pyarrow as pa

import ray
from ray._common.utils import env_integer, get_or_create_event_loop
from ray.data._internal.compute import ActorPoolStrategy, ComputeStrategy, get_compute
from ray.data._internal.execution.bundle_queue import ExactMultipleSize, RebundleQueue
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BatchMapTransformFn,
    BlockMapTransformFn,
    MapTransformCallable,
    MapTransformer,
    Row,
    RowMapTransformFn,
)
from ray.data._internal.execution.util import make_callable_class_single_threaded
from ray.data._internal.logical.operators import (
    AbstractUDFMap,
    Filter,
    FlatMap,
    MapBatches,
    MapRows,
    Project,
    StreamingRepartition,
)
from ray.data._internal.numpy_support import _is_valid_column_values
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.util import _truncated_repr
from ray.data.block import (
    Block,
    BlockAccessor,
    CallableClass,
    DataBatch,
    UserDefinedFunction,
    _is_cudf_dataframe,
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

# Controls how many outputs could be buffered for a *single* in-flight async UDF
# invocation.
#
# NOTE: Async UDFs are unrolled inside the task (to maintain requested concurrency
#       level), hence this buffer has to be bounded to prevent peak memory
#       utilization from scaling with the number of objects yielded by a single
#       UDF invocation (instead it's bounded by `target_max_block_size` applied
#       by the downstream block-shaping stage).
DEFAULT_ASYNC_UDF_MAX_BUFFERED_OUTPUTS = env_integer(
    "RAY_DATA_DEFAULT_ASYNC_UDF_MAX_BUFFERED_OUTPUTS", 1
)


@dataclass
class UDFSpec:
    """Specification for a callable class UDF to be instantiated in an actor.

    Attributes:
        spec: The callable class specification (contains class and constructor args)
        instantiation_class: The class to instantiate (may be wrapped, e.g., for concurrency)
    """

    spec: "_CallableClassSpec"
    instantiation_class: type


class _MapActorContext:
    def __init__(
        self,
        is_async: bool = False,
        udf_instances: Optional[Dict[int, Any]] = None,
    ):
        """Initialize the map actor context.

        Args:
            is_async: Whether any UDF is async
            udf_instances: Dict mapping UDF class ID to instantiated instance
        """
        self.is_async = is_async
        self.udf_map_asyncio_loop = None
        self.udf_map_asyncio_thread = None
        self.udf_instances = udf_instances or {}

        if is_async:
            self._init_async()

    def _init_async(self):
        # Only used for callable class with async generator `__call__` method.
        loop = get_or_create_event_loop()

        def run_loop():
            asyncio.set_event_loop(loop)
            loop.run_forever()

        thread = Thread(target=run_loop, daemon=True)
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

    # Extract expressions before defining the closure to prevent cloudpickle from
    # serializing the entire op object (which may contain references to non-serializable
    # datasources with weak references, e.g., PyIceberg tables)
    projection_exprs = op.exprs
    common_sub_exprs = op.get_common_sub_exprs()

    compute = get_compute(op.compute)

    # Create init_fn to initialize all callable class UDFs at actor startup
    from ray.data.util.expression_utils import (
        _create_callable_class_udf_init_fn,
    )

    init_fn = _create_callable_class_udf_init_fn(op.get_all_exprs())

    def _project_block(block: Block) -> Block:
        try:
            from ray.data._internal.planner.plan_expression.expression_evaluator import (
                eval_projection,
            )

            return eval_projection(
                projection_exprs,
                block,
                common_sub_exprs=common_sub_exprs,
            )
        except Exception as e:
            _try_wrap_udf_exception(e)

    map_transformer = MapTransformer(
        [
            BlockMapTransformFn(
                _generate_transform_fn_for_map_block(_project_block),
                disable_block_shaping=(len(op.exprs) == 0),
            )
        ],
        init_fn=init_fn,
    )
    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name=op.name,
        compute_strategy=compute,
        ray_remote_args=op.ray_remote_args,
        ray_remote_args_fn=op.ray_remote_args_fn,
    )


def plan_streaming_repartition_op(
    op: StreamingRepartition,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]
    compute = get_compute(op.compute)
    transform_fn = BlockMapTransformFn(
        lambda blocks, ctx: blocks,
        output_block_size_option=OutputBlockSizeOption.of(
            target_num_rows_per_block=op.target_num_rows_per_block,  # To split n*target_max_block_size row into n blocks
        ),
    )
    map_transformer = MapTransformer([transform_fn])

    if op.strict:
        ref_bundler = RebundleQueue(ExactMultipleSize(op.target_num_rows_per_block))
    else:
        ref_bundler = None

    operator = MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name=op.name,
        compute_strategy=compute,
        ref_bundler=ref_bundler,
        ray_remote_args=op.ray_remote_args,
        ray_remote_args_fn=op.ray_remote_args_fn,
    )

    return operator


def plan_filter_op(
    op: Filter,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    output_block_size_option = OutputBlockSizeOption.of(
        target_max_block_size=data_context.target_max_block_size,
    )

    predicate_expr = op.predicate_expr
    compute = get_compute(op.compute)
    if predicate_expr is not None:

        def filter_block_fn(
            blocks: Iterable[Block], ctx: TaskContext
        ) -> Iterable[Block]:
            for block in blocks:
                block_accessor = BlockAccessor.for_block(block)
                filtered_block = block_accessor.filter(predicate_expr)
                yield filtered_block

        init_fn = None
        transform_fn = BlockMapTransformFn(
            filter_block_fn,
            is_udf=True,
            output_block_size_option=output_block_size_option,
        )
    else:
        udf_is_callable_class = isinstance(op.fn, CallableClass)
        filter_fn, init_fn = _get_udf(
            op.fn,
            op.fn_args,
            op.fn_kwargs,
            op.fn_constructor_args if udf_is_callable_class else None,
            op.fn_constructor_kwargs if udf_is_callable_class else None,
            compute=compute,
        )

        transform_fn = RowMapTransformFn(
            _generate_transform_fn_for_filter(filter_fn),
            is_udf=True,
            output_block_size_option=output_block_size_option,
        )

    map_transformer = MapTransformer([transform_fn], init_fn=init_fn)

    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name=op.name,
        compute_strategy=compute,
        ray_remote_args=op.ray_remote_args,
        ray_remote_args_fn=op.ray_remote_args_fn,
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

    output_block_size_option = OutputBlockSizeOption.of(
        target_max_block_size=data_context.target_max_block_size,
    )

    compute = get_compute(op.compute)
    udf_is_callable_class = isinstance(op.fn, CallableClass)
    fn, init_fn = _get_udf(
        op.fn,
        op.fn_args,
        op.fn_kwargs,
        op.fn_constructor_args if udf_is_callable_class else None,
        op.fn_constructor_kwargs if udf_is_callable_class else None,
        compute=compute,
    )

    if isinstance(op, MapBatches):
        transform_fn = BatchMapTransformFn(
            _generate_transform_fn_for_map_batches(fn),
            batch_size=op.batch_size,
            batch_format=op.batch_format,
            zero_copy_batch=op.zero_copy_batch,
            is_udf=True,
            output_block_size_option=output_block_size_option,
        )

    else:
        if isinstance(op, MapRows):
            udf_fn = _generate_transform_fn_for_map_rows(fn)
        elif isinstance(op, FlatMap):
            udf_fn = _generate_transform_fn_for_flat_map(fn)
        else:
            raise ValueError(f"Found unknown logical operator during planning: {op}")

        transform_fn = RowMapTransformFn(
            udf_fn,
            is_udf=True,
            output_block_size_option=output_block_size_option,
        )

    map_transformer = MapTransformer([transform_fn], init_fn=init_fn)

    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name=op.name,
        compute_strategy=compute,
        min_rows_per_bundle=op.min_rows_per_bundled_input,
        ray_remote_args_fn=op.ray_remote_args_fn,
        ray_remote_args=op.ray_remote_args,
        per_block_limit=op.per_block_limit,
    )


def _get_udf(
    op_fn: Callable,
    op_fn_args: Tuple[Any, ...],
    op_fn_kwargs: Dict[str, Any],
    op_fn_constructor_args: Optional[Tuple[Any, ...]],
    op_fn_constructor_kwargs: Optional[Dict[str, Any]],
    compute: Optional[ComputeStrategy],
):
    # Note, it's important to define these standalone variables.
    # So the parsed functions won't need to capture the entire operator, which may not
    # be serializable.
    udf = op_fn
    fn_args = op_fn_args or ()
    fn_kwargs = op_fn_kwargs or {}

    if isinstance(udf, CallableClass):
        from ray.data.expressions import _CallableClassSpec

        fn_constructor_args = op_fn_constructor_args or ()
        fn_constructor_kwargs = op_fn_constructor_kwargs or {}

        is_async_udf = _is_async_udf(udf.__call__)

        # Capture original class BEFORE wrapping for use as dict key
        original_udf_class = udf

        if (
            not is_async_udf
            and isinstance(compute, ActorPoolStrategy)
            and not compute.enable_true_multi_threading
        ):
            # NOTE: By default Actor-based UDFs are restricted to run within a
            # single-thread (when enable_true_multi_threading=False).
            #
            # Historically, this has been done to allow block-fetching, batching, etc to
            # be overlapped with the actual UDF invocation, while avoiding the
            # pitfalls of concurrent GPU access (like OOMs, etc) when specifying
            # max_concurrency > 1.
            udf = make_callable_class_single_threaded(udf)

        # Create the callable class spec for this UDF
        callable_class_spec = _CallableClassSpec(
            cls=original_udf_class,
            args=fn_constructor_args,
            kwargs=fn_constructor_kwargs,
        )

        # Use the shared init function creator (handles both map_batches and expressions)
        init_fn = create_actor_context_init_fn(
            udf_specs=[UDFSpec(spec=callable_class_spec, instantiation_class=udf)]
        )

        # Capture the spec for lookup on the actor
        captured_spec = callable_class_spec

        if inspect.iscoroutinefunction(udf.__call__):
            # Async coroutine UDF: wrapper must be async to work with async transform machinery
            async def _wrapped_udf_map_fn(item: Any) -> Any:
                assert ray.data._map_actor_context is not None
                assert ray.data._map_actor_context.is_async

                try:
                    # Use spec's key for lookup
                    udf_key = captured_spec.make_key()
                    udf_instance = ray.data._map_actor_context.udf_instances[udf_key]
                    # Direct await - already in async context
                    return await udf_instance(
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
                    # Use spec's key for lookup
                    udf_key = captured_spec.make_key()
                    udf_instance = ray.data._map_actor_context.udf_instances[udf_key]
                    gen = udf_instance(
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
                    # Use spec's key for lookup
                    udf_key = captured_spec.make_key()
                    udf_instance = ray.data._map_actor_context.udf_instances[udf_key]
                    return udf_instance(
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
    allowed = isinstance(
        batch,
        (
            list,
            pa.Table,
            np.ndarray,
            collections.abc.Mapping,
            pd.core.frame.DataFrame,
            dict,
        ),
    ) or _is_cudf_dataframe(batch)
    if not allowed:
        raise ValueError(
            "The `fn` you passed to `map_batches` returned a value of type "
            f"{type(batch)}. This isn't allowed -- `map_batches` expects "
            "`fn` to return a `pandas.DataFrame`, `pyarrow.Table`, "
            "`cudf.DataFrame`, `numpy.ndarray`, `list`, or "
            "`dict[str, numpy.ndarray]`."
        )

    if isinstance(batch, list):
        raise ValueError(
            f"Error validating {_truncated_repr(batch)}: "
            "Returning a list of objects from `map_batches` is not "
            "allowed in Ray 2.5. To return Python objects, "
            "wrap them in a named dict field, e.g., "
            "return `{'results': objects}` instead of just `objects`."
        )

    # Handle cudf.DataFrame before the Mapping check, since cudf.DataFrame
    # implements the Mapping protocol. Mirrors the order in batch_to_block.
    if _is_cudf_dataframe(batch):
        return

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


class _TransformingBatchIterator(Iterator[DataBatch]):
    """Iterator that applies a UDF to batches.

    Unlike a generator, local variables in __next__ go out of scope when the method
    returns, avoiding holding references to yielded values.

    Uses a deque with popleft() to actually release references when items are consumed,
    rather than keeping them in an iterator.
    """

    def __init__(self, batches: Iterable[DataBatch], fn: UserDefinedFunction):
        self._input_iter = iter(batches)
        self._fn = fn
        self._cur_output_iter: Optional[Iterator[DataBatch]] = None

    def __iter__(self) -> "_TransformingBatchIterator":
        return self

    def __next__(self) -> DataBatch:
        while True:
            # Check if there's pending output iter we'd continue fetching
            # from
            if self._cur_output_iter is not None:
                try:
                    out_batch = next(self._cur_output_iter)
                except StopIteration:
                    pass
                else:
                    _validate_batch_output(out_batch)
                    return out_batch

            # Fetch the next batch from upstream
            input_batch = next(self._input_iter)

            if (
                not isinstance(input_batch, collections.abc.Mapping)
                and not _is_cudf_dataframe(input_batch)
                and BlockAccessor.for_block(input_batch).num_rows() == 0
            ):
                # For empty input blocks, we directly output them without
                # calling the UDF.
                # TODO(hchen): This workaround is because some all-to-all
                # operators output empty blocks with no schema.
                self._cur_output_iter = _ReleasingIterator(
                    collections.deque([input_batch])
                )
            else:
                try:
                    res = self._fn(input_batch)

                    if not isinstance(res, GeneratorType):
                        # NOTE: It's critical that we're utilizing *releasing* iterator
                        #       to avoid capturing intermediate objects along the whole
                        #       iterator chain
                        self._cur_output_iter = _ReleasingIterator(
                            collections.deque([res])
                        )
                    else:
                        # In cases when UDF returns a generator we iterate over it
                        # as is (given that we can't release intermediate state from
                        # UDF anyway)
                        self._cur_output_iter = res
                except ValueError as e:
                    read_only_msgs = [
                        "assignment destination is read-only",
                        "buffer source array is read-only",
                    ]
                    err_msg = str(e)
                    if any(msg in err_msg for msg in read_only_msgs):
                        raise ValueError(
                            f"Batch mapper function {self._fn.__name__} tried to mutate a "
                            "zero-copy read-only batch. To be able to mutate the "
                            "batch, pass zero_copy_batch=False to map_batches(); "
                            "this will create a writable copy of the batch before "
                            "giving it to fn. To elide this copy, modify your mapper "
                            "function so it doesn't try to mutate its input."
                        ) from e
                    else:
                        raise e from None


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
            return _TransformingBatchIterator(batches, fn)

    return transform_fn


def _is_async_udf(fn: UserDefinedFunction) -> bool:
    return inspect.iscoroutinefunction(fn) or inspect.isasyncgenfunction(fn)


def create_actor_context_init_fn(
    udf_specs: List[UDFSpec],
):
    """Create an init function for registering callable class UDFs in actor context.

    This is the shared core logic between map_batches (single UDF) and expressions (multiple UDFs).

    Args:
        udf_specs: List of UDF specifications

    Returns:
        An init function that sets up all UDFs in the actor context
    """

    def init_fn():
        import ray

        if ray.data._map_actor_context is None:
            # Check if any UDF is async
            has_async_udf = any(
                _is_async_udf(spec.instantiation_class.__call__) for spec in udf_specs
            )

            # Create instances for all callable class UDFs
            udf_instances = {}
            for spec in udf_specs:
                # Use the spec's key for deduplication and lookup
                udf_key = spec.spec.make_key()
                if udf_key not in udf_instances:
                    # Instantiate using the wrapped/processed class
                    udf_instances[udf_key] = spec.instantiation_class(
                        *spec.spec.args, **spec.spec.kwargs
                    )

            # Single unified context for all UDFs
            ray.data._map_actor_context = _MapActorContext(
                is_async=has_async_udf,
                udf_instances=udf_instances,
            )

    return init_fn


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


_SENTINEL = object()

T = TypeVar("T")
U = TypeVar("U")


def _generate_transform_fn_for_async_map(
    fn: UserDefinedFunction,
    validate_fn: Callable,
    *,
    max_concurrency: int,
    is_flat_map: bool = False,
    max_buffered_udf_outputs: int = DEFAULT_ASYNC_UDF_MAX_BUFFERED_OUTPUTS,
) -> MapTransformCallable:
    assert max_concurrency > 0, "Max concurrency must be positive"
    assert (
        max_buffered_udf_outputs > 0
    ), "Max number of buffered UDF outputs must be positive"

    if inspect.isasyncgenfunction(fn):

        async def _apply_udf(item: T, udf_output_queue: asyncio.Queue) -> None:
            gen = fn(item)
            try:
                # NOTE: Async generator is unrolled inside the task to maintain
                #       requested concurrency level (`max_concurrency`).
                #
                #       Outputs are, however, streamed into a *bounded* queue (rather
                #       than accumulated in a list) so that unrolling gets
                #       back-pressured, and peak memory utilization doesn't scale
                #       with the number of objects yielded by a single invocation.
                async for out in gen:
                    await udf_output_queue.put(out)
            finally:
                await udf_output_queue.put(_SENTINEL)

    elif inspect.iscoroutinefunction(fn):

        async def _apply_udf(item: T, udf_output_queue: asyncio.Queue) -> None:
            try:
                res = await fn(item)
                for out in res if is_flat_map else [res]:
                    await udf_output_queue.put(out)
            finally:
                await udf_output_queue.put(_SENTINEL)

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
    #   - Peak memory utilization *must not* scale with the number of objects
    #     produced by an individual UDF invocation (ie an async generator yielding
    #     N blocks should not require N blocks to be held in memory)
    #
    # To achieve that, algorithm applying async UDF to elements of the provided sequence
    # is structured like following:
    #
    #   - Task scheduling and subsequent results reporting are performed as
    #     different stages (inside `_execute_transform` and `_report` respectively)
    #
    #   - Scheduling stage aim to schedule and run no more than `max_concurrency` tasks
    #     at any given moment
    #
    #   - Every scheduled task is handed its own output queue that it streams its
    #     outputs into. This queue is capped at `max_buffered_udf_outputs` (+1 slot
    #     for the terminating sentinel), so that a task producing multiple outputs
    #     (ie an async generator) gets back-pressured instead of buffering all of
    #     them at once.
    #
    #   - Scheduled tasks are added into the scheduled tasks queue (in the order of the
    #     input sequence), for their outputs to be subsequently reported. Since tasks
    #     are both scheduled and reported in the order of the input sequence, resulting
    #     ordering is *always* deterministic (no reordering stage is necessary).
    #
    #   - Number of the tasks that have been scheduled, but not yet reported is
    #     capped at `2 * max_concurrency` to make sure scheduling stage is throttled
    #     (and buffered outputs aren't growing unbounded) in case when reporting
    #     stage isn't able to keep up.
    #
    #   - Reporting stage dequeues scheduled tasks (in order) and drains their
    #     respective output queues into the output queue.
    #
    #   - Output queue is capped at `maxsize=max_concurrency` elements to make sure that
    #     reporting stage is throttled (and output queue doesn't grow unbounded) in case
    #     when consumer (Ray task itself) isn't able to keep up
    #
    async def _execute_transform(it: Iterator[T], output_queue: queue.Queue) -> None:
        loop = asyncio.get_running_loop()

        # NOTE: Tasks are enqueued here upon being *scheduled* (as opposed to upon
        #       completion): a task streaming into a bounded output queue might only
        #       be able to complete once its outputs have been drained by the
        #       reporting stage.
        #
        #       This queue doesn't need to be capped: its size is bounded by
        #       `unreported_tasks_sema` (keeping it uncapped avoids scheduling stage
        #       blocking on it in case reporting stage terminated early).
        scheduled_tasks_queue = asyncio.Queue()
        # NOTE: Caps the number of tasks that have been scheduled, but haven't been
        #       reported yet (ie are either still running, or have already completed
        #       with their outputs still buffered).
        #
        #       This allows tasks completing out of order to release their
        #       concurrency slots *without* waiting to be reported, while still
        #       bounding the amount of the outputs buffered in between the stages.
        unreported_tasks_sema = asyncio.Semaphore(2 * max_concurrency)
        # NOTE: This method is nested to support Python 3.9 where we only can
        #       init `asyncio.Queue` inside the async function
        async def _report() -> None:
            try:
                while True:
                    task, udf_output_queue = await scheduled_tasks_queue.get()

                    # NOTE: Scheduling stage captures `BaseException` (not just
                    #       `Exception`), hence the sentinel it hands over could be
                    #       one as well (for ex, `asyncio.CancelledError`). Matching
                    #       on `Exception` only would let it fall through into
                    #       draining a `None` output queue, masking the original
                    #       failure with an `AttributeError`.
                    if isinstance(task, BaseException):
                        raise task
                    elif task is _SENTINEL:
                        break

                    while True:
                        out = await udf_output_queue.get()
                        if out is _SENTINEL:
                            break

                        # NOTE: Once output queue fills up, this will block
                        #       therefore serving as back-pressure for the UDF
                        #       task producing into `udf_output_queue`, and
                        #       transitively for the scheduling stage.
                        # NOTE: This will block the whole event-loop not just this task
                        output_queue.put(out)

                    # NOTE: Task is awaited to surface any exception it might have
                    #       raised (it's already completed at this point, since it
                    #       terminated its output queue)
                    await task

                    unreported_tasks_sema.release()

                sentinel = _SENTINEL

            except BaseException as e:
                sentinel = e
            finally:
                # NOTE: Scheduling stage could be awaiting on the semaphore, hence
                #       it has to be released (unconditionally) to avoid it getting
                #       stuck in case reporting stage terminated early
                for _ in range(2 * max_concurrency):
                    unreported_tasks_sema.release()

                output_queue.put(sentinel)

        # NOTE: Reporting is an async process. Keep a strong reference to
        # the created task: ``loop.create_task`` only registers a weak
        # reference with the event loop, so without a strong reference the
        # task could be garbage collected mid-execution and the reporting
        # would silently stop.
        report_task = loop.create_task(_report())

        cur_task_map: Dict[asyncio.Task, asyncio.Queue] = dict()
        consumed = False

        sentinel = _SENTINEL

        try:
            while True:
                while len(cur_task_map) < max_concurrency and not consumed:
                    try:
                        item = next(it)
                    except StopIteration:
                        consumed = True
                        break

                    # NOTE: Once the number of tasks awaiting to be reported reaches
                    #       the cap, this will block therefore serving as
                    #       back-pressure for scheduling stage
                    await unreported_tasks_sema.acquire()

                    # Launch async task providing it with a bounded queue to
                    # stream its outputs into
                    #
                    # NOTE: Extra slot is reserved for the terminating sentinel, so
                    #       that tasks producing no more than
                    #       `max_buffered_udf_outputs` outputs (in particular, plain
                    #       coroutine UDFs) could complete (and therefore release
                    #       their concurrency slots) w/o waiting to be reported
                    udf_output_queue = asyncio.Queue(
                        maxsize=max_buffered_udf_outputs + 1
                    )
                    task = loop.create_task(_apply_udf(item, udf_output_queue))
                    cur_task_map[task] = udf_output_queue

                    # NOTE: Scheduled tasks are reported in the order of the input
                    #       sequence, hence produced ordering is deterministic
                    scheduled_tasks_queue.put_nowait((task, udf_output_queue))

                # Check if any running tasks remaining
                if not cur_task_map:
                    break

                # NOTE: Reporting task is awaited alongside the UDF ones to make
                #       sure scheduling stage isn't stuck indefinitely in case
                #       reporting stage terminated early (for ex, upon UDF failing),
                #       and therefore won't be draining tasks' output queues anymore
                done, _ = await asyncio.wait(
                    cur_task_map.keys() | {report_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if report_task in done:
                    break

                for task in done:
                    cur_task_map.pop(task)

        except BaseException as e:
            sentinel = e
        finally:
            if report_task.done():
                # Reporting stage is not going to drain remaining tasks' output
                # queues, hence these tasks have to be cancelled.
                #
                # NOTE: Buffered outputs are dropped as well, to make sure cancelled
                #       tasks aren't blocked (indefinitely) putting into a full queue
                for cur_task, udf_output_queue in cur_task_map.items():
                    if not cur_task.done():
                        cur_task.cancel()

                    while not udf_output_queue.empty():
                        udf_output_queue.get_nowait()

            scheduled_tasks_queue.put_nowait((sentinel, None))
            # Wait for the reporting task to finish draining ``scheduled_tasks_queue``
            # and pushing remaining results to the output queue. This both keeps a
            # strong reference to the task alive until completion (preventing GC)
            # and surfaces any unexpected exception raised inside ``_report``.
            await report_task

    def _transform(batch_iter: Iterable[T], task_context: TaskContext) -> Iterable[U]:
        output_queue = queue.Queue(maxsize=max_concurrency)

        loop = ray.data._map_actor_context.udf_map_asyncio_loop

        asyncio.run_coroutine_threadsafe(
            _execute_transform(iter(batch_iter), output_queue), loop
        )

        while True:
            item = output_queue.get()
            if item is _SENTINEL:
                break
            # NOTE: Reporting stage captures `BaseException`, hence the sentinel it
            #       hands over could be one as well (see `_report`)
            elif isinstance(item, BaseException):
                raise item
            else:
                validate_fn(item)
                yield item

    return _transform


class _ReleasingIterator(Iterator[T]):
    def __init__(self, d: collections.deque):
        self._d = d

    def __iter__(self):
        return self

    def __next__(self):
        if not self._d:
            raise StopIteration

        return self._d.popleft()
