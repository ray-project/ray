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
    Union,
)

import numpy as np
import pandas as pd
import pyarrow as pa

import ray
from ray._common.utils import get_or_create_event_loop
from ray._private.ray_constants import env_integer
from ray.data._internal.compute import get_compute
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
from ray.data._internal.output_buffer import OutputBlockSizeOption
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
        udf_map_cls: Optional[UserDefinedFunction] = None,
        udf_map_fn: Optional[Callable[[Any], Any]] = None,
        is_async: bool = False,
        udf_instances: Optional[Dict[int, Any]] = None,
    ):
        """Initialize the map actor context.

        Args:
            udf_map_cls: For map_batches - the callable class type
            udf_map_fn: For map_batches - the instantiated callable class instance
            is_async: Whether the UDF is async
            udf_instances: For expressions - dict mapping UDF ID to instance
        """
        self.udf_map_cls = udf_map_cls
        self.udf_map_fn = udf_map_fn
        self.is_async = is_async
        self.udf_map_asyncio_loop = None
        self.udf_map_asyncio_thread = None
        # For expression evaluation with callable class UDFs
        self.udf_instances = udf_instances or {}

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

    # Extract op.exprs before defining the closure to prevent cloudpickle from
    # serializing the entire op object (which may contain references to non-serializable
    # datasources with weak references, e.g., PyIceberg tables)
    projection_exprs = op.exprs

    # Collect all callable class UDFs from expressions
    from ray.data._internal.planner.plan_expression.expression_visitors import (
        _CallableClassUDFCollector,
    )

    callable_class_udfs = []
    for expr in op.exprs:
        collector = _CallableClassUDFCollector()
        collector.visit(expr)
        callable_class_udfs.extend(collector.get_callable_class_udfs())

    # Create init_fn by reusing the shared actor context init logic
    init_fn = None
    if callable_class_udfs:
        # Build UDF specs from expressions
        # Apply the same preprocessing as _get_udf (make_callable_class_concurrent)
        udf_specs = []
        seen_udf_classes = (
            {}
        )  # Map original class ID to (original_class, wrapped_class)

        for udf_expr in callable_class_udfs:
            orig_udf_class = udf_expr.fn_constructor_class
            orig_udf_id = id(orig_udf_class)

            # Only process each unique UDF class once
            if orig_udf_id not in seen_udf_classes:
                # Apply make_callable_class_concurrent for non-async UDFs (same as _get_udf)
                if not _is_async_udf(orig_udf_class.__call__):
                    wrapped_udf_class = make_callable_class_concurrent(orig_udf_class)
                else:
                    wrapped_udf_class = orig_udf_class

                seen_udf_classes[orig_udf_id] = (orig_udf_class, wrapped_udf_class)
                # Store both original class (for lookup key) and wrapped class (for instantiation)
                udf_specs.append(
                    (
                        orig_udf_class,  # Original class - used as key for lookup
                        wrapped_udf_class,  # Wrapped class - used for instantiation
                        udf_expr.fn_constructor_args,
                        udf_expr.fn_constructor_kwargs,
                    )
                )

        # Use the shared init function creator (same core logic as _get_udf)
        # No primary_udf_class for expressions (only needed for map_batches backward compat)
        init_fn = create_actor_context_init_fn(udf_specs, primary_udf_class=None)

    def _project_block(block: Block) -> Block:
        try:
            from ray.data._internal.planner.plan_expression.expression_evaluator import (
                eval_projection,
            )

            return eval_projection(projection_exprs, block)
        except Exception as e:
            _try_wrap_udf_exception(e)

    compute = get_compute(op._compute)
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

    # Create a no-op transform that is just coalescing/slicing the incoming
    # blocks
    transform_fn = BlockMapTransformFn(
        lambda blocks, ctx: blocks,
        output_block_size_option=OutputBlockSizeOption.of(
            target_num_rows_per_block=op.target_num_rows_per_block
        ),
    )

    map_transformer = MapTransformer([transform_fn])

    # Disable fusion for streaming repartition with the downstream op.
    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name=op.name,
        compute_strategy=compute,
        min_rows_per_bundle=op.target_num_rows_per_block,
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

    output_block_size_option = OutputBlockSizeOption.of(
        target_max_block_size=data_context.target_max_block_size,
    )

    predicate_expr = op._predicate_expr
    compute = get_compute(op._compute)
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
        udf_is_callable_class = isinstance(op._fn, CallableClass)
        filter_fn, init_fn = _get_udf(
            op._fn,
            op._fn_args,
            op._fn_kwargs,
            op._fn_constructor_args if udf_is_callable_class else None,
            op._fn_constructor_kwargs if udf_is_callable_class else None,
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

    output_block_size_option = OutputBlockSizeOption.of(
        target_max_block_size=data_context.target_max_block_size,
    )

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
        transform_fn = BatchMapTransformFn(
            _generate_transform_fn_for_map_batches(fn),
            batch_size=op._batch_size,
            batch_format=op._batch_format,
            zero_copy_batch=op._zero_copy_batch,
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
        min_rows_per_bundle=op._min_rows_per_bundled_input,
        ray_remote_args_fn=op._ray_remote_args_fn,
        ray_remote_args=op._ray_remote_args,
        per_block_limit=op._per_block_limit,
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

        # Use the shared init function creator (handles both map_batches and expressions)
        # For map_batches, pass the udf as primary_udf_class to maintain backward compatibility
        init_fn = create_actor_context_init_fn(
            udf_specs=[(udf, fn_constructor_args, fn_constructor_kwargs)],
            primary_udf_class=udf,  # Makes init_fn also populate udf_map_fn field
        )

        if inspect.iscoroutinefunction(udf.__call__):
            # Async coroutine UDF: wrapper must be async to work with async transform machinery
            async def _wrapped_udf_map_fn(item: Any) -> Any:
                assert ray.data._map_actor_context is not None
                assert ray.data._map_actor_context.is_async

                try:
                    # Direct await - already in async context
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


def _call_udf_instance_with_async_bridge(
    instance: Any,
    async_loop: Optional[Any],
    *args,
    **kwargs,
) -> Any:
    """Call a UDF instance, bridging from sync context to async if needed.

    This is the shared async calling logic used by both map_batches and expressions.
    Extracted from the core logic in _get_udf's wrapper functions.

    Args:
        instance: The callable instance to call
        async_loop: The async event loop (if available)
        *args: Positional arguments
        **kwargs: Keyword arguments

    Returns:
        The result of calling the instance
    """
    import asyncio

    # Check if the instance's __call__ is async
    if inspect.iscoroutinefunction(instance.__call__):
        # Async coroutine: bridge from sync to async
        if async_loop is not None:
            future = asyncio.run_coroutine_threadsafe(
                instance(*args, **kwargs), async_loop
            )
            return future.result()
        else:
            return asyncio.run(instance(*args, **kwargs))
    elif inspect.isasyncgenfunction(instance.__call__):
        # Async generator: collect results
        async def _collect():
            results = []
            async for item in instance(*args, **kwargs):
                results.append(item)
            # Mimic map_batches behavior: collect all yields
            # In map_batches: return [out async for out in gen] - each yield becomes a separate batch
            # For expressions: we need one value per row, so return the last yield (most recent/final)
            # This matches the semantic intent while adapting to expression evaluation context
            if not results:
                return None
            elif len(results) == 1:
                return results[0]
            else:
                import logging

                logging.warning(
                    f"Async generator yielded {len(results)} values in expression context; "
                    "only the last (most recent) is returned. Use map_batches for multi-yield support."
                )
                # Multiple values yielded - return the last one (most recent/final result)
                # Unlike map_batches where each yield becomes a separate output, expressions
                # require a single value per row, so we take the final yield as the result.
                return results[-1]

        if async_loop is not None:
            future = asyncio.run_coroutine_threadsafe(_collect(), async_loop)
            return future.result()
        else:
            return asyncio.run(_collect())
    else:
        # Synchronous instance - direct call
        return instance(*args, **kwargs)


def call_udf_from_actor_context(
    udf_constructor_class: Optional[type],
    args: List[Any],
    kwargs: Dict[str, Any],
) -> Optional[Any]:
    """Call a UDF from actor context if available.

    This reuses the same instance lookup and calling logic as map_batches.

    Args:
        udf_constructor_class: The original callable class type
        args: Positional arguments to pass to the UDF
        kwargs: Keyword arguments to pass to the UDF

    Returns:
        The result of calling the UDF, or None if not in actor context
    """
    import ray

    if (
        udf_constructor_class is not None
        and ray.data._map_actor_context is not None
        and ray.data._map_actor_context.udf_instances
    ):
        # Get the instance from the actor context
        udf_id = id(udf_constructor_class)
        if udf_id in ray.data._map_actor_context.udf_instances:
            instance = ray.data._map_actor_context.udf_instances[udf_id]
            # Use the shared async bridge helper
            return _call_udf_instance_with_async_bridge(
                instance,
                ray.data._map_actor_context.udf_map_asyncio_loop,
                *args,
                **kwargs,
            )

    return None  # Caller should handle fallback


def create_actor_context_init_fn(
    udf_specs: List[
        Union[
            Tuple[
                type, Tuple[Any, ...], Dict[str, Any]
            ],  # map_batches format: (class, args, kwargs)
            Tuple[
                type, type, Tuple[Any, ...], Dict[str, Any]
            ],  # expressions format: (orig_class, wrapped_class, args, kwargs)
        ]
    ],
    primary_udf_class: Optional[type] = None,
):
    """Create an init function for registering callable class UDFs in actor context.

    This is the shared core logic between map_batches (single UDF) and expressions (multiple UDFs).

    Args:
        udf_specs: List of UDF specifications. Two formats supported:
                   - (udf_class, constructor_args, constructor_kwargs) for map_batches
                   - (original_class, wrapped_class, constructor_args, constructor_kwargs) for expressions
        primary_udf_class: For map_batches backward compatibility - the main UDF class
                           whose instance should also be stored in udf_map_fn

    Returns:
        An init function that sets up all UDFs in the actor context
    """

    def init_fn():
        import ray

        if ray.data._map_actor_context is None:
            # Normalize specs to (original_class, instantiation_class, args, kwargs)
            normalized_specs = []
            for spec in udf_specs:
                if len(spec) == 3:
                    # map_batches format: (class, args, kwargs)
                    udf_class, ctor_args, ctor_kwargs = spec
                    normalized_specs.append(
                        (udf_class, udf_class, ctor_args, ctor_kwargs)
                    )
                else:
                    # expressions format: already (orig_class, wrapped_class, args, kwargs)
                    normalized_specs.append(spec)

            # Check if any UDF is async (check the instantiation class)
            has_async_udf = any(
                _is_async_udf(inst_class.__call__)
                for _, inst_class, _, _ in normalized_specs
            )

            # Create instances for all callable class UDFs
            udf_instances = {}
            for orig_class, inst_class, ctor_args, ctor_kwargs in normalized_specs:
                # Use id of the ORIGINAL class as the key (for expression lookup)
                udf_id = id(orig_class)
                if udf_id not in udf_instances:
                    # Instantiate using the wrapped/processed class
                    udf_instances[udf_id] = inst_class(*ctor_args, **ctor_kwargs)

            # For map_batches backward compatibility, also set udf_map_cls and udf_map_fn
            if primary_udf_class is not None:
                # Get the primary instance from the instances dict
                primary_instance = udf_instances[id(primary_udf_class)]
                ray.data._map_actor_context = _MapActorContext(
                    udf_map_cls=primary_udf_class,
                    udf_map_fn=primary_instance,
                    is_async=has_async_udf,
                    udf_instances=udf_instances,
                )
            else:
                # Expressions path - only udf_instances dict
                ray.data._map_actor_context = _MapActorContext(
                    udf_instances=udf_instances, is_async=has_async_udf
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
