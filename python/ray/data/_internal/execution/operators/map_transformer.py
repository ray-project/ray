import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional, TypeVar, Union

from ray.data._internal.block_batching.block_batching import batch_blocks
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.output_buffer import BlockOutputBuffer, OutputBlockSizeOption
from ray.data.block import BatchFormat, Block, BlockAccessor, DataBatch

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


class MapTransformFn(ABC):
    """Represents a single transform function in a MapTransformer."""

    def __init__(
        self,
        input_type: MapTransformFnDataType,
        *,
        is_udf: bool = False,
        output_block_size_option: Optional[OutputBlockSizeOption] = None,
    ):
        """
        Args:
            input_type: Expected type of the input data.
            is_udf: Whether this transformation is UDF or not.
            output_block_size_option: (Optional) Output block size configuration.
        """
        self._input_type = input_type
        self._output_block_size_option = output_block_size_option
        self._is_udf = is_udf

    @abstractmethod
    def _post_process(self, results: Iterable[MapTransformFnData]) -> Iterable[Block]:
        pass

    @abstractmethod
    def _apply_transform(
        self, ctx: TaskContext, inputs: Iterable[MapTransformFnData]
    ) -> Iterable[MapTransformFnData]:
        pass

    def _pre_process(self, blocks: Iterable[Block]) -> Iterable[MapTransformFnData]:
        return blocks

    def _shape_blocks(self, results: Iterable[MapTransformFnData]) -> Iterable[Block]:
        buffer = BlockOutputBuffer(self._output_block_size_option)

        # This method supports following modes of shaping of the output blocks:
        #
        #   1. Incremental: block is accumulated up to configured
        #      ``_output_block_size_option``
        #
        #   2. *Non-incremental* (aka 1 block in / 1 block out): when
        #      no ``OutputBlockSizeOption`` is provided this method will absorb
        #      the whole input sequence and produce single block as an output
        #
        if self._input_type == MapTransformFnDataType.Block:
            append = buffer.add_block
        elif self._input_type == MapTransformFnDataType.Batch:
            append = buffer.add_batch
        else:
            assert self._input_type == MapTransformFnDataType.Row
            append = buffer.add

        # Iterate over input sequence appending results to the
        # buffer, while yielding incrementally
        for result in results:
            append(result)
            # Try yielding incrementally
            while buffer.has_next():
                yield buffer.next()
        # Finalize buffer
        buffer.finalize()
        # Yield remaining blocks from it
        while buffer.has_next():
            yield buffer.next()

    def __call__(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> Iterable[Block]:
        batches = self._pre_process(blocks)
        results = self._apply_transform(ctx, batches)
        yield from self._post_process(results)

    @property
    def output_block_size_option(self):
        return self._output_block_size_option

    def override_target_max_block_size(self, target_max_block_size: Optional[int]):
        if self._output_block_size_option is not None and (
            self._output_block_size_option.disable_block_shaping
            or self._output_block_size_option.target_num_rows_per_block is not None
        ):
            raise ValueError(
                "Cannot override target_max_block_size if block shaping is disabled or target_num_rows_per_block is set"
            )
        self._output_block_size_option = OutputBlockSizeOption.of(
            target_max_block_size=target_max_block_size
        )

    @property
    def target_max_block_size(self):
        if self._output_block_size_option is None:
            return None
        else:
            return self._output_block_size_option.target_max_block_size

    @property
    def target_num_rows_per_block(self):
        if self._output_block_size_option is None:
            return None
        else:
            return self._output_block_size_option.target_num_rows_per_block


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
        *,
        init_fn: Optional[Callable[[], None]] = None,
        output_block_size_option_override: Optional[OutputBlockSizeOption] = None,
    ):
        """
        Args:
            transform_fns: A list of `MapTransformFn`s that will be executed sequentially
                to transform data.
            init_fn: A function that will be called before transforming data.
                Used for the actor-based map operator.
            output_block_size_option_override: (Optional) Output block size configuration.
        """

        self._transform_fns = []
        self._init_fn = init_fn if init_fn is not None else lambda: None
        self._output_block_size_option_override = output_block_size_option_override
        self._udf_time = 0

        # Add transformations
        self.add_transform_fns(transform_fns)

    def add_transform_fns(self, transform_fns: List[MapTransformFn]) -> None:
        """Set the transform functions."""
        assert len(transform_fns) > 0
        self._transform_fns = self._combine_transformations(
            self._transform_fns, transform_fns
        )

    def get_transform_fns(self) -> List[MapTransformFn]:
        """Get the transform functions."""
        return self._transform_fns

    def override_target_max_block_size(self, target_max_block_size: Optional[int]):
        self._output_block_size_option_override = OutputBlockSizeOption.of(
            target_max_block_size=target_max_block_size
        )

    @property
    def target_max_block_size_override(self) -> Optional[int]:
        if self._output_block_size_option_override is None:
            return None
        else:
            return self._output_block_size_option_override.target_max_block_size

    def init(self) -> None:
        """Initialize the transformer.

        Should be called before applying the transform.
        """
        self._init_fn()

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

        # NOTE: We only need to configure last transforming function to do
        #       appropriate block sizing
        last_transform = self._transform_fns[-1]

        if self.target_max_block_size_override is not None:
            last_transform.override_target_max_block_size(
                self.target_max_block_size_override
            )

        iter = input_blocks
        # Apply the transform functions sequentially to the input iterable.
        for transform_fn in self._transform_fns:
            iter = transform_fn(iter, ctx)
            if transform_fn._is_udf:
                iter = self._udf_timed_iter(iter)

        return iter

    def fuse(self, other: "MapTransformer") -> "MapTransformer":
        """Fuse two `MapTransformer`s together."""
        assert (
            self.target_max_block_size_override == other.target_max_block_size_override
            or (
                self.target_max_block_size_override is None
                or other.target_max_block_size_override is None
            )
        )
        # Define them as standalone variables to avoid fused_init_fn capturing the
        # entire `MapTransformer` object.
        self_init_fn = self._init_fn
        other_init_fn = other._init_fn

        def fused_init_fn():
            self_init_fn()
            other_init_fn()

        combined_transform_fns = self._combine_transformations(
            self._transform_fns,
            other._transform_fns,
        )

        transformer = MapTransformer(
            combined_transform_fns,
            init_fn=fused_init_fn,
            output_block_size_option_override=OutputBlockSizeOption.of(
                target_max_block_size=(
                    self.target_max_block_size_override
                    or other.target_max_block_size_override
                ),
            ),
        )

        return transformer

    @classmethod
    def _combine_transformations(
        cls, ones: List[MapTransformFn], others: List[MapTransformFn]
    ) -> list[Any]:
        return ones + others

    def udf_time(self) -> float:
        return self._udf_time


# Below are subclasses of MapTransformFn.


class RowMapTransformFn(MapTransformFn):
    """A rows-to-rows MapTransformFn."""

    def __init__(
        self,
        row_fn: MapTransformCallable[Row, Row],
        *,
        is_udf: bool = False,
        output_block_size_option: OutputBlockSizeOption,
    ):
        super().__init__(
            input_type=MapTransformFnDataType.Row,
            is_udf=is_udf,
            output_block_size_option=output_block_size_option,
        )

        self._row_fn = row_fn

    def _pre_process(self, blocks: Iterable[Block]) -> Iterable[MapTransformFnData]:
        for block in blocks:
            block = BlockAccessor.for_block(block)
            for row in block.iter_rows(public_row_format=True):
                yield row

    def _apply_transform(
        self, ctx: TaskContext, inputs: Iterable[MapTransformFnData]
    ) -> Iterable[MapTransformFnData]:
        yield from self._row_fn(inputs, ctx)

    def _post_process(self, results: Iterable[MapTransformFnData]) -> Iterable[Block]:
        return self._shape_blocks(results)

    def __repr__(self) -> str:
        return f"RowMapTransformFn({self._row_fn})"


class BatchMapTransformFn(MapTransformFn):
    """A batch-to-batch MapTransformFn."""

    def __init__(
        self,
        batch_fn: MapTransformCallable[DataBatch, DataBatch],
        *,
        is_udf: bool = False,
        batch_size: Optional[int] = None,
        batch_format: Optional[BatchFormat] = None,
        zero_copy_batch: bool = True,
        output_block_size_option: Optional[OutputBlockSizeOption] = None,
    ):
        super().__init__(
            input_type=MapTransformFnDataType.Batch,
            is_udf=is_udf,
            output_block_size_option=output_block_size_option,
        )

        self._batch_size = batch_size
        self._batch_format = batch_format
        self._zero_copy_batch = zero_copy_batch
        self._ensure_copy = not zero_copy_batch and batch_size is not None

        self._batch_fn = batch_fn

    def _pre_process(self, blocks: Iterable[Block]) -> Iterable[MapTransformFnData]:
        # TODO make batch-udf zero-copy by default
        ensure_copy = not self._zero_copy_batch and self._batch_size is not None

        return batch_blocks(
            blocks=iter(blocks),
            stats=None,
            batch_size=self._batch_size,
            batch_format=self._batch_format,
            ensure_copy=ensure_copy,
        )

    def _apply_transform(
        self, ctx: TaskContext, batches: Iterable[MapTransformFnData]
    ) -> Iterable[MapTransformFnData]:
        yield from self._batch_fn(batches, ctx)

    def _post_process(self, results: Iterable[MapTransformFnData]) -> Iterable[Block]:
        return self._shape_blocks(results)

    def __repr__(self) -> str:
        return f"BatchMapTransformFn({self._batch_fn=}, {self._batch_format=}, {self._batch_size=}, {self._zero_copy_batch=})"


class BlockMapTransformFn(MapTransformFn):
    """A block-to-block MapTransformFn."""

    def __init__(
        self,
        block_fn: MapTransformCallable[Block, Block],
        *,
        is_udf: bool = False,
        disable_block_shaping: bool = False,
        output_block_size_option: Optional[OutputBlockSizeOption] = None,
    ):
        """
        Initializes the object with a transformation function, accompanying options, and
        configuration for handling blocks during processing.

        Args:
            block_fn: Callable function to apply a transformation to a block.
            is_udf: Specifies if the transformation function is a user-defined
                function (defaults to ``False``).
            disable_block_shaping: Disables block-shaping, making transformer to
                produce blocks as is.
            output_block_size_option: (Optional) Configure output block sizing.
        """

        super().__init__(
            input_type=MapTransformFnDataType.Block,
            is_udf=is_udf,
            output_block_size_option=output_block_size_option,
        )

        self._block_fn = block_fn
        self._disable_block_shaping = disable_block_shaping

    def _apply_transform(
        self, ctx: TaskContext, blocks: Iterable[Block]
    ) -> Iterable[Block]:
        yield from self._block_fn(blocks, ctx)

    def _post_process(self, results: Iterable[MapTransformFnData]) -> Iterable[Block]:
        # Short-circuit for block transformations for which no
        # block-shaping is required
        if self._disable_block_shaping:
            return results

        return self._shape_blocks(results)

    def __repr__(self) -> str:
        return (
            f"BlockMapTransformFn({self._block_fn=}, {self._output_block_size_option=})"
        )
