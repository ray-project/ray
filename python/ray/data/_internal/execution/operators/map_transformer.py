import asyncio
import collections
import inspect
import itertools
import logging
import queue
import time
from abc import abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from threading import Thread
from types import GeneratorType
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, TypeVar, Union

import numpy as np
import pandas as pd
import pyarrow as pa

import ray
from ray._common.utils import get_or_create_event_loop
from ray.data._internal.block_batching.block_batching import batch_blocks
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.util import make_callable_class_concurrent
from ray.data._internal.numpy_support import _is_valid_column_values
from ray.data._internal.output_buffer import BlockOutputBuffer, OutputBlockSizeOption
from ray.data._internal.util import _truncated_repr
from ray.data.block import (
    Block,
    BlockAccessor,
    CallableClass,
    DataBatch,
    UserDefinedFunction,
)
from ray.exceptions import UserCodeException
from ray.util.rpdb import _is_ray_debugger_post_mortem_enabled

logger = logging.getLogger(__name__)


# Allowed input/output data types for a MapTransformFn.
Row = Dict[str, Any]
MapTransformFnData = Union[Block, Row, DataBatch]

# Function signature of a MapTransformFn.
IN = TypeVar("IN")
OUT = TypeVar("OUT")
MapTransformCallable = Callable[[Iterable[IN], TaskContext], Iterable[OUT]]


class MapTransformFnDataType(Enum):
    """An enum that represents the input/output data type of a MapTransformFn."""

    Block = 0
    Row = 1
    Batch = 2


class MapTransformFnCategory(Enum):
    """An enum that represents the PreProcess/DataProcess/PostProcess category of a
    MapTransformFn.
    """

    # Data format conversion before the actual data processing, i.e. converting input blocks to rows or batches.
    PreProcess = 0

    # Actual Data processing/transformation.
    DataProcess = 1

    # Data format conversion after the actual data processing, i.e., converting rows or batches to output blocks.
    PostProcess = 2


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


@dataclass
class MapTransformUDFContext:
    op_fn: UserDefinedFunction
    fn_args: Tuple[Any, ...] = field(default_factory=tuple)
    fn_kwargs: Dict[str, Any] = field(default_factory=dict)
    fn_constructor_args: Tuple[Any, ...] = field(default_factory=tuple)
    fn_constructor_kwargs: Dict[str, Any] = field(default_factory=dict)
    is_async: bool = field(default=False)
    map_actor_context: Optional[_MapActorContext] = None


# TODO: deduplicate with ray.data._internal.planner.plan_udf_map_op.py
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


class MapTransformFn:
    """Represents a single transform function in a MapTransformer."""

    def __init__(
        self,
        input_type: MapTransformFnDataType,
        output_type: MapTransformFnDataType,
        category: MapTransformFnCategory,
        udf_context: Optional[MapTransformUDFContext] = None,
    ):
        """
        Args:
            callable: the underlying Python callable object.
            input_type: the type of the input data.
            output_type: the type of the output data.
        """
        self._callable = callable
        self._input_type = input_type
        self._output_type = output_type
        self._category = category
        self._output_block_size_option = None
        self._is_udf = udf_context is not None
        self._udf_context = udf_context

    @abstractmethod
    def __call__(
        self,
        input: Iterable[MapTransformFnData],
        ctx: TaskContext,
    ) -> Iterable[MapTransformFnData]:
        ...

    def init(self) -> None:
        # default implementation for MapTransformFn's that do not need initialization
        pass

    @property
    def input_type(self) -> MapTransformFnDataType:
        return self._input_type

    @property
    def output_type(self) -> MapTransformFnDataType:
        return self._output_type

    @property
    def category(self) -> MapTransformFnCategory:
        return self._category

    @property
    def output_block_size_option(self):
        return self._output_block_size_option

    def set_target_max_block_size(self, target_max_block_size: int):
        assert target_max_block_size is not None
        self._output_block_size_option = OutputBlockSizeOption(
            target_max_block_size=target_max_block_size
        )

    @property
    def target_max_block_size(self):
        if self._output_block_size_option is None:
            return None
        else:
            return self._output_block_size_option.target_max_block_size

    def set_target_num_rows_per_block(self, target_num_rows_per_block: int):
        assert target_num_rows_per_block is not None
        self._output_block_size_option = OutputBlockSizeOption(
            target_num_rows_per_block=target_num_rows_per_block
        )

    @property
    def target_num_rows_per_block(self):
        if self._output_block_size_option is None:
            return None
        else:
            return self._output_block_size_option.target_num_rows_per_block

    def __repr__(self) -> str:
        op_fn_name = self._udf_context.op_fn.__name__ if self._udf_context else None
        return f"{self.__class__.__name__}({op_fn_name}[{self._input_type} -> {self._output_type}])"

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self._udf_context.op_fn == other._udf_context.op_fn
            and self._input_type == other._input_type
            and self._output_type == other._output_type
            and self._udf_context.is_async == other._udf_context.is_async
            and self._is_udf == other._is_udf
        )


class MapTransformer:
    """Encapsulates the data transformation logic of a physical MapOperator.

    A MapTransformer may consist of one or more steps, each of which is represented
    as a MapTransformFn. The first MapTransformFn must take blocks as input, and
    the last MapTransformFn must output blocks. The intermediate data types can
    be blocks, rows, or batches.
    """

    def __init__(
        self,
        transform_fns: List[MapTransformFn],
        init_fn: Optional[Callable[[], None]] = None,
    ):
        """
        Args:
        transform_fns: A list of `MapTransformFn`s that will be executed sequentially
            to transform data.
        init_fn: A function that will be called before transforming data.
            Used for the actor-based map operator.
        """
        self.set_transform_fns(transform_fns)

        self._init_fn = init_fn if init_fn is not None else lambda: None
        self._output_block_size_option = None
        self._udf_time = 0
        self._initialized = False

    def set_transform_fns(self, transform_fns: List[MapTransformFn]) -> None:
        """Set the transform functions."""
        assert len(transform_fns) > 0
        assert (
            transform_fns[0].input_type == MapTransformFnDataType.Block
        ), "The first transform function must take blocks as input."
        assert (
            transform_fns[-1].output_type == MapTransformFnDataType.Block
        ), "The last transform function must output blocks."

        for i in range(len(transform_fns) - 1):
            assert transform_fns[i].output_type == transform_fns[i + 1].input_type, (
                "The output type of the previous transform function must match "
                "the input type of the next transform function."
            )
        self._transform_fns = transform_fns

    def get_transform_fns(self) -> List[MapTransformFn]:
        """Get the transform functions."""
        return self._transform_fns

    def set_target_max_block_size(self, target_max_block_size: int):
        if target_max_block_size is not None:
            self._output_block_size_option = OutputBlockSizeOption(
                target_max_block_size=target_max_block_size
            )
        elif self._output_block_size_option is not None:
            self._output_block_size_option = None

    @property
    def target_max_block_size(self):
        if self._output_block_size_option is None:
            return None
        else:
            return self._output_block_size_option.target_max_block_size

    def set_target_num_rows_per_block(self, target_num_rows_per_block: int):
        assert (
            self._output_block_size_option is None
            and target_num_rows_per_block is not None
        )
        self._output_block_size_option = OutputBlockSizeOption(
            target_num_rows_per_block=target_num_rows_per_block
        )

    @property
    def target_num_rows_per_block(self):
        if self._output_block_size_option is None:
            return None
        else:
            return self._output_block_size_option.target_num_rows_per_block

    def init(self) -> None:
        """Initialize the transformer.

        Should be called before applying the transform.
        """
        if not self._initialized:
            for transform_fn in self._transform_fns:
                transform_fn.init()
            self._initialized = True

    def _udf_timed_iter(
        self, input: Iterable[MapTransformFnData]
    ) -> Iterable[MapTransformFnData]:
        while True:
            try:
                start = time.perf_counter()
                output = next(input)
                self._udf_time += time.perf_counter() - start
                yield output
            except StopIteration:
                break

    def apply_transform(
        self,
        input_blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> Iterable[Block]:
        """Apply the transform functions to the input blocks."""
        self.init()
        assert (
            self.target_max_block_size is not None
        ), "target_max_block_size must be set before running"
        for transform_fn in self._transform_fns:
            if not transform_fn.output_block_size_option:
                transform_fn.set_target_max_block_size(self.target_max_block_size)

        iter = input_blocks
        # Apply the transform functions sequentially to the input iterable.
        for transform_fn in self._transform_fns:
            iter = transform_fn(iter, ctx)
            if transform_fn._is_udf:
                iter = self._udf_timed_iter(iter)
        return iter

    def fuse(self, other: "MapTransformer") -> "MapTransformer":
        """Fuse two `MapTransformer`s together."""
        assert self.target_max_block_size == other.target_max_block_size or (
            self.target_max_block_size is None or other.target_max_block_size is None
        )
        target_max_block_size = (
            self.target_max_block_size or other.target_max_block_size
        )

        # Define them as standalone variables to avoid fused_init_fn capturing the
        # entire `MapTransformer` object.
        self_init_fn = self._init_fn
        other_init_fn = other._init_fn

        def fused_init_fn():
            self_init_fn()
            other_init_fn()

        fused_transform_fns = self._transform_fns + other._transform_fns
        transformer = MapTransformer(fused_transform_fns, init_fn=fused_init_fn)
        transformer.set_target_max_block_size(target_max_block_size)
        return transformer

    def udf_time(self) -> float:
        return self._udf_time


def create_map_transformer_from_block_fn(
    block_fn: MapTransformCallable[Block, Block],
    init_fn: Optional[Callable[[], None]] = None,
):
    """Create a MapTransformer from a single block-based transform function.

    This method should only be used for testing and legacy compatibility.
    """
    return MapTransformer(
        [
            BlockMapTransformFn(block_fn),
        ],
        init_fn,
    )


# Below are subclasses of MapTransformFn.


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


def _validate_row_output(item):
    if not isinstance(item, collections.abc.Mapping):
        raise ValueError(
            f"Error validating {_truncated_repr(item)}: "
            "Standalone Python objects are not "
            "allowed in Ray 2.5. To return Python objects from map(), "
            "wrap them in a dict, e.g., "
            "return `{'item': item}` instead of just `item`."
        )


class MapTransformFnOpType(Enum):
    """An enum that represents the underlying operator type of a MapTransformFn."""

    MapBatches = 0
    MapRows = 1
    FlatMap = 2
    MapBlocks = 3
    Filter = 4


class AbstractUDFMapTransformFn(MapTransformFn):
    def __init__(
        self,
        udf_context: MapTransformUDFContext,
        input_type: MapTransformFnDataType,
        output_type: MapTransformFnDataType,
        map_transform_fn_type: MapTransformFnOpType,
    ):
        super().__init__(
            input_type,
            output_type,
            MapTransformFnCategory.DataProcess,
            udf_context,
        )
        self._map_transform_fn_type = map_transform_fn_type

    def _generate_transform_fn(
        self, fn: Callable[[Any], Any]
    ) -> Callable[[Iterable, TaskContext], Iterable]:
        if self._map_transform_fn_type == MapTransformFnOpType.MapBatches:

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
        elif self._map_transform_fn_type == MapTransformFnOpType.MapRows:

            def transform_fn(rows: Iterable[Row], _: TaskContext) -> Iterable[Row]:
                for row in rows:
                    out_row = fn(row)
                    _validate_row_output(out_row)
                    yield out_row

            return transform_fn
        elif self._map_transform_fn_type == MapTransformFnOpType.FlatMap:

            def transform_fn(rows: Iterable[Row], _: TaskContext) -> Iterable[Row]:
                for row in rows:
                    for out_row in fn(row):
                        _validate_row_output(out_row)
                        yield out_row

            return transform_fn
        elif self._map_transform_fn_type == MapTransformFnOpType.Filter:

            def transform_fn(rows: Iterable[Row], _: TaskContext) -> Iterable[Row]:
                for row in rows:
                    if fn(row):
                        yield row

            return transform_fn
        elif self._map_transform_fn_type == MapTransformFnOpType.MapBlocks:

            def transform_fn(
                blocks: Iterable[Block], _: TaskContext
            ) -> Iterable[Block]:
                for block in blocks:
                    out_block = fn(block)
                    yield out_block

            return transform_fn
        else:
            raise ValueError(
                f"Invalid MapTransformFnType input type: {self._input_type}"
            )


class FunctionUDFMapTransformFn(AbstractUDFMapTransformFn):
    """A MapTransformFn that applies a UDF. If the UDF is a callable class, use CallableClassUDFMapTransformFn or AsyncCallableClassUDFMapTransformFn."""

    def __init__(
        self,
        udf_context: MapTransformUDFContext,
        input_type: MapTransformFnDataType,
        output_type: MapTransformFnDataType,
        map_transform_fn_type: MapTransformFnOpType,
    ):
        super().__init__(
            udf_context,
            input_type,
            output_type,
            map_transform_fn_type,
        )

    def __call__(
        self, input: Iterable[MapTransformFnData], ctx: TaskContext
    ) -> Iterable[MapTransformFnData]:
        yield from self._transform_fn(input, ctx)

    def init(self) -> None:
        def fn(item: Any) -> Any:
            try:
                return self._udf_context.op_fn(
                    item, *self._udf_context.fn_args, **self._udf_context.fn_kwargs
                )
            except Exception as e:
                _handle_debugger_exception(e, item)

        self._transform_fn = self._generate_transform_fn(fn)


class CallableClassUDFMapTransformFn(AbstractUDFMapTransformFn):
    """A MapTransformFn that applies a callable class UDF. If the UDF is async, use AsyncCallableClassUDFMapTransformFn."""

    def __init__(
        self,
        udf_context: MapTransformUDFContext,
        input_type: MapTransformFnDataType,
        output_type: MapTransformFnDataType,
        map_transform_fn_type: MapTransformFnOpType,
    ):
        super().__init__(
            udf_context,
            input_type,
            output_type,
            map_transform_fn_type,
        )

    def __call__(
        self, input: Iterable[MapTransformFnData], ctx: TaskContext
    ) -> Iterable[MapTransformFnData]:
        yield from self._transform_fn(input, ctx)

    def init(self) -> None:
        assert isinstance(self._udf_context.op_fn, CallableClass)
        assert not self._udf_context.is_async
        op_fn = self._udf_context.op_fn
        op_fn = make_callable_class_concurrent(op_fn)
        self._udf_context.map_actor_context = _MapActorContext(
            udf_map_cls=op_fn,
            udf_map_fn=op_fn(
                *self._udf_context.fn_constructor_args,
                **self._udf_context.fn_constructor_kwargs,
            ),
            is_async=self._udf_context.is_async,
        )

        def fn(item: Any) -> Any:
            assert self._udf_context.map_actor_context is not None
            assert not self._udf_context.map_actor_context.is_async
            try:
                return self._udf_context.map_actor_context.udf_map_fn(
                    item,
                    *self._udf_context.fn_args,
                    **self._udf_context.fn_kwargs,
                )
            except Exception as e:
                _handle_debugger_exception(e, item)

        self._transform_fn = self._generate_transform_fn(fn)


class AsyncCallableClassUDFMapTransformFn(MapTransformFn):
    """A MapTransformFn that applies a asynchronous callable class UDF. If the UDF is not async, use CallableClassUDFMapTransformFn."""

    def __init__(
        self,
        udf_context: MapTransformUDFContext,
        input_type: MapTransformFnDataType,
        output_type: MapTransformFnDataType,
        map_transform_fn_type: MapTransformFnOpType,
    ):
        super().__init__(
            input_type,
            output_type,
            category=MapTransformFnCategory.DataProcess,
            udf_context=udf_context,
        )

    def __call__(
        self, input: Iterable[MapTransformFnData], ctx: TaskContext
    ) -> Iterable[MapTransformFnData]:
        yield from self._transform_fn(input, ctx)

    def init(self) -> None:
        assert isinstance(self._udf_context.op_fn, CallableClass)
        assert self._udf_context.is_async
        op_fn = self._udf_context.op_fn
        self._udf_context.map_actor_context = _MapActorContext(
            udf_map_cls=op_fn,
            udf_map_fn=op_fn(
                *self._udf_context.fn_constructor_args,
                **self._udf_context.fn_constructor_kwargs,
            ),
            is_async=self._udf_context.is_async,
        )

        async def fn(item: Any) -> Any:
            assert self._udf_context.map_actor_context is not None
            assert self._udf_context.map_actor_context.is_async

            try:
                return self._udf_context.map_actor_context.udf_map_fn(
                    item,
                    *self._udf_context.fn_args,
                    **self._udf_context.fn_kwargs,
                )
            except Exception as e:
                _handle_debugger_exception(e, item)

        validate_fn = (
            _validate_row_output
            if self._input_type == MapTransformFnDataType.Row
            else _validate_batch_output
        )
        self._transform_fn = self._generate_transform_fn(fn, validate_fn)

    def _generate_transform_fn(
        self, fn: Callable[[Any], Any], validate_fn: Callable[[Any], None]
    ) -> Callable[[Iterable, TaskContext], Iterable]:
        # Generates a transform function for asynchronous mapping of items (either batches or rows)
        # using a user-defined function (UDF). This consolidated function handles both asynchronous
        # batch processing and asynchronous flat mapping (e.g., rows) based on the provided UDF.
        assert inspect.iscoroutinefunction(fn)

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
                    loop = self._udf_context.map_actor_context.udf_map_asyncio_loop
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
            loop = self._udf_context.map_actor_context.udf_map_asyncio_loop
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


class BlocksToRowsMapTransformFn(MapTransformFn):
    """A MapTransformFn that converts input blocks to rows."""

    def __init__(self):
        super().__init__(
            MapTransformFnDataType.Block,
            MapTransformFnDataType.Row,
            category=MapTransformFnCategory.PreProcess,
        )

    def __call__(self, blocks: Iterable[Block], _: TaskContext) -> Iterable[Row]:
        for block in blocks:
            block = BlockAccessor.for_block(block)
            for row in block.iter_rows(public_row_format=True):
                yield row

    @classmethod
    def instance(cls) -> "BlocksToRowsMapTransformFn":
        """Returns the singleton instance."""
        if getattr(cls, "_instance", None) is None:
            cls._instance = cls()
        return cls._instance

    def __repr__(self) -> str:
        return "BlocksToRowsMapTransformFn()"

    def __eq__(self, other):
        return isinstance(other, BlocksToRowsMapTransformFn)


class BlocksToBatchesMapTransformFn(MapTransformFn):
    """A MapTransformFn that converts input blocks to batches."""

    def __init__(
        self,
        batch_size: Optional[int] = None,
        batch_format: str = "default",
        zero_copy_batch: bool = False,
    ):
        self._batch_size = batch_size
        self._batch_format = batch_format
        self._ensure_copy = not zero_copy_batch and batch_size is not None
        super().__init__(
            MapTransformFnDataType.Block,
            MapTransformFnDataType.Batch,
            category=MapTransformFnCategory.PreProcess,
        )

    def __call__(
        self,
        blocks: Iterable[Block],
        _: TaskContext,
    ) -> Iterable[DataBatch]:
        """Converts input blocks to batches."""
        block_iter = iter(blocks)
        first = next(block_iter, None)
        if first is None:
            return []
        blocks = itertools.chain([first], block_iter)
        empty_block = BlockAccessor.for_block(first).builder().build()
        # Don't hold the first block in memory, so we reset the reference.
        first = None

        # Ensure that zero-copy batch views are copied so mutating UDFs don't error.
        formatted_batch_iter = batch_blocks(
            blocks=blocks,
            stats=None,
            batch_size=self._batch_size,
            batch_format=self._batch_format,
            ensure_copy=self._ensure_copy,
        )

        first = next(formatted_batch_iter, None)
        if first is None:
            # If the input blocks are all empty, then yield an empty block with same
            # format as the input blocks.
            return [empty_block]
        else:
            return itertools.chain([first], formatted_batch_iter)

    @property
    def batch_size(self) -> Optional[int]:
        return self._batch_size

    @property
    def batch_format(self) -> str:
        return self._batch_format

    @property
    def zero_copy_batch(self) -> bool:
        return not self._ensure_copy

    def __repr__(self) -> str:
        return (
            f"BlocksToBatchesMapTransformFn("
            f"batch_size={self._batch_size}, "
            f"batch_format={self._batch_format}, "
            f"zero_copy_batch={self.zero_copy_batch}"
            f")"
        )

    def __eq__(self, other):
        return (
            isinstance(other, BlocksToBatchesMapTransformFn)
            and self.batch_format == other.batch_format
            and self.batch_size == other.batch_size
            and self.zero_copy_batch == other.zero_copy_batch
        )


class BuildOutputBlocksMapTransformFn(MapTransformFn):
    """A MapTransformFn that converts UDF-returned data to output blocks."""

    def __init__(self, input_type: MapTransformFnDataType):
        """
        Args:
            input_type: the type of input data.
        """
        self._input_type = input_type
        super().__init__(
            input_type,
            MapTransformFnDataType.Block,
            category=MapTransformFnCategory.PostProcess,
        )

    def __call__(
        self,
        iter: Iterable[MapTransformFnData],
        _: TaskContext,
    ) -> Iterable[Block]:
        """Convert UDF-returned data to output blocks.

        Args:
            iter: the iterable of UDF-returned data, whose type
                must match self._input_type.
        """
        output_buffer = BlockOutputBuffer(self.output_block_size_option)
        if self._input_type == MapTransformFnDataType.Block:
            add_fn = output_buffer.add_block
        elif self._input_type == MapTransformFnDataType.Batch:
            add_fn = output_buffer.add_batch
        else:
            assert self._input_type == MapTransformFnDataType.Row
            add_fn = output_buffer.add
        for data in iter:
            add_fn(data)
            while output_buffer.has_next():
                yield output_buffer.next()
        output_buffer.finalize()
        while output_buffer.has_next():
            yield output_buffer.next()

    @classmethod
    def for_rows(cls) -> "BuildOutputBlocksMapTransformFn":
        """Return a BuildOutputBlocksMapTransformFn for row input."""
        return cls(MapTransformFnDataType.Row)

    @classmethod
    def for_batches(cls) -> "BuildOutputBlocksMapTransformFn":
        """Return a BuildOutputBlocksMapTransformFn for batch input."""
        return cls(MapTransformFnDataType.Batch)

    @classmethod
    def for_blocks(cls) -> "BuildOutputBlocksMapTransformFn":
        """Return a BuildOutputBlocksMapTransformFn for block input."""
        return cls(MapTransformFnDataType.Block)

    def __repr__(self) -> str:
        return f"BuildOutputBlocksMapTransformFn(input_type={self._input_type})"

    def __eq__(self, other):
        return (
            isinstance(other, BuildOutputBlocksMapTransformFn)
            and self.input_type == other.input_type
        )


def _splitrange(n, k):
    """Calculates array lens of np.array_split().

    This is the equivalent of
    `[len(x) for x in np.array_split(range(n), k)]`.
    """
    base = n // k
    output = [base] * k
    rem = n - sum(output)
    for i in range(len(output)):
        if rem > 0:
            output[i] += 1
            rem -= 1
    assert rem == 0, (rem, output, n, k)
    assert sum(output) == n, (output, n, k)
    return output


class ApplyAdditionalSplitToOutputBlocks(MapTransformFn):
    """Do additional splits on output blocks."""

    def __init__(self, additional_split_factor: int):
        """
        Args:
          additional_output_splits: The number of additional splits, must be
             greater than 1.
        """
        assert additional_split_factor > 1
        self._additional_split_factor = additional_split_factor
        super().__init__(
            MapTransformFnDataType.Block,
            MapTransformFnDataType.Block,
            category=MapTransformFnCategory.PostProcess,
        )

    def __call__(self, blocks: Iterable[Block], ctx: TaskContext) -> Iterable[Block]:
        for block in blocks:
            block = BlockAccessor.for_block(block)
            offset = 0
            split_sizes = _splitrange(block.num_rows(), self._additional_split_factor)
            for size in split_sizes:
                # NOTE: copy=True is needed because this is an output block. If
                # a block slice is put into the object store, the entire block
                # will get serialized.
                yield block.slice(offset, offset + size, copy=True)
                offset += size


class BlockMapTransformFn(MapTransformFn):
    """A block-to-block MapTransformFn."""

    def __init__(self, block_fn: MapTransformCallable[Block, Block]):
        self._block_fn = block_fn
        super().__init__(
            MapTransformFnDataType.Block,
            MapTransformFnDataType.Block,
            category=MapTransformFnCategory.DataProcess,
        )

    def __call__(self, input: Iterable[Block], ctx: TaskContext) -> Iterable[Block]:
        yield from self._block_fn(input, ctx)

    def __repr__(self) -> str:
        return f"BlockMapTransformFn({self._block_fn})"

    def __eq__(self, other):
        return (
            isinstance(other, BlockMapTransformFn) and self._block_fn == other._block_fn
        )
