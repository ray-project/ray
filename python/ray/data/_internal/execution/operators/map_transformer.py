import itertools
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

from ray._common.utils import env_integer
from ray.data._internal.block_batching.block_batching import batch_blocks
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.output_buffer import BlockOutputBuffer, OutputBlockSizeOption
from ray.data.block import (
    BatchFormat,
    Block,
    BlockAccessor,
    CustomOpStats,
    DataBatch,
)

_DEFAULT_BATCH_SIZE_BYTES: int = env_integer(
    "RAY_DATA_DEFAULT_BATCH_SIZE_BYTES", 16 * 1024 * 1024  # 16 MiB
)

# Allowed input/output data types for a MapTransformFn.
Row = Dict[str, Any]
MapTransformFnData = Union[Block, Row, DataBatch]


class CustomOpStatsReporter:
    """Per-task reporter that carries transforms' :class:`CustomOpStats`.

    ``_map_task`` creates one per task and threads it into the transform chain.
    Each producing transform calls ``op_stats_reporter.report(stats)`` once,
    before yielding output blocks, to append its :class:`CustomOpStats` to the
    reporter. Fused transforms each contribute one entry, so the reporter holds a
    list. ``_map_task`` reads :meth:`get_stats` after each output block and stamps
    the list onto the block metadata as part of ``TaskExecWorkerStats``
    """

    def __init__(self) -> None:
        self._stats: List[CustomOpStats] = []

    def report(self, stats: CustomOpStats) -> None:
        """Append a producing transform's per-task CustomOpStats."""
        self._stats.append(stats)

    def get_stats(self) -> List[CustomOpStats]:
        """Return all reported CustomOpStats (empty if none were reported)."""
        return self._stats

    def clear(self) -> None:
        """Drop any reported stats (called before each task attempt)."""
        self._stats = []


# Narrow callback handed to producing transforms to report per-task
# :class:`CustomOpStats`.
CustomOpStatsReportFn = Callable[[CustomOpStats], None]


def _noop_report_custom_op_stats(stats: CustomOpStats) -> None:
    """Stateless default report callback for callers that don't collect stats."""


IN = TypeVar("IN")
OUT = TypeVar("OUT")
# A transform callable accepts either ``(data, ctx)`` or, when it reports
# per-task CustomOpStats, ``(data, ctx, report_custom_op_stats)``.
MapTransformCallable = Union[
    Callable[[Iterable[IN], TaskContext], Iterable[OUT]],
    Callable[[Iterable[IN], TaskContext, CustomOpStatsReportFn], Iterable[OUT]],
]


class MapTransformFnDataType(Enum):
    """An enum that represents the input/output data type of a MapTransformFn."""

    Block = 0
    Row = 1
    Batch = 2


class MapTransformFn(ABC):
    """Represents a single transform function in a MapTransformer."""

    def __init__(
        self,
        fn: Callable,
        input_type: MapTransformFnDataType,
        *,
        is_udf: bool = False,
        output_block_size_option: Optional[OutputBlockSizeOption] = None,
        should_report_custom_op_stats: bool = False,
    ):
        """Initialize a :class:`MapTransformFn`.

        Args:
            fn: The wrapped transform callable. Invoked with ``(data, ctx)``, or
                ``(data, ctx, report_custom_op_stats)`` when
                ``should_report_custom_op_stats=True``.
            input_type: Expected type of the input data.
            is_udf: Whether this transformation is UDF or not.
            output_block_size_option: (Optional) Output block size configuration.
            should_report_custom_op_stats: If ``True``, the wrapped callable accepts a
                third ``report_custom_op_stats`` callback argument and may report
                per-task :class:`CustomOpStats` to the driver. Defaults to
                ``False``, in which case the callable is invoked with
                ``(data, ctx)`` only.
        """
        self._fn = fn
        self._input_type = input_type
        self._output_block_size_option = output_block_size_option
        self._is_udf = is_udf
        self._should_report_custom_op_stats = should_report_custom_op_stats

    @abstractmethod
    def _post_process(self, results: Iterable[MapTransformFnData]) -> Iterable[Block]:
        pass

    def _apply_transform(
        self,
        ctx: TaskContext,
        inputs: Iterable[MapTransformFnData],
        report_custom_op_stats: CustomOpStatsReportFn = _noop_report_custom_op_stats,
    ) -> Iterable[MapTransformFnData]:
        """Call the wrapped fn, passing ``report_custom_op_stats`` only if it opted in.

        Keeps the common ``(data, ctx)`` signature for the vast majority of
        transforms; only those constructed with ``should_report_custom_op_stats=True``
        receive the report callback.
        """
        if self._should_report_custom_op_stats:
            return self._fn(inputs, ctx, report_custom_op_stats)
        return self._fn(inputs, ctx)

    def _pre_process(self, blocks: Iterable[Block]) -> Iterable[MapTransformFnData]:
        return blocks

    def _shape_blocks(self, results: Iterable[MapTransformFnData]) -> Iterable[Block]:
        """Shape results into blocks using a buffer."""
        return _BlockShapingIterator(
            results, self._input_type, self._output_block_size_option
        )

    def timed_steps(
        self,
        ctx: TaskContext,
        report_custom_op_stats: CustomOpStatsReportFn,
        stage_idx: int,
    ) -> List["TimedStep"]:
        """This transform as a flat list of timed pipeline steps.

        `_pre_process` and `_post_process` are already `Iterable -> Iterable`,
        so they are the step bodies as-is; only the wrapped fn needs a closure
        to bind the task context.
        """
        name = repr(self)
        body = MapTransformPhase.UDF_BODY if self._is_udf else MapTransformPhase.OTHER
        return [
            TimedStep(
                f"{name} input prep",
                MapTransformPhase.INPUT_PREP,
                stage_idx,
                self._pre_process,
            ),
            TimedStep(
                f"{name} body",
                body,
                stage_idx,
                lambda it: self._apply_transform(ctx, it, report_custom_op_stats),
            ),
            TimedStep(
                f"{name} output build",
                MapTransformPhase.OUTPUT_BUILD,
                stage_idx,
                self._post_process,
            ),
        ]

    def __call__(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
        report_custom_op_stats: CustomOpStatsReportFn = _noop_report_custom_op_stats,
    ) -> Iterable[Block]:
        """Apply this transform's steps in order, untimed.

        Timing belongs to the chain rather than to a transform:
        :meth:`MapTransformer.apply_transform` wraps these same steps in a
        :class:`TransformClock`. A caller driving one transform on its own --
        ``generate_collect_write_stats_fn``, and tests -- gets the plain
        composition.
        """
        data: Iterable[Any] = blocks
        for step in self.timed_steps(ctx, report_custom_op_stats, 0):
            data = step.apply(data)
        return data

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


class MapTransformPhase(Enum):
    """The phases a :class:`MapTransformFn` splits its work into.

    Steps carry one of these; the summary groups their times by it.
    """

    # Turning input blocks into the batches or rows the transform consumes.
    INPUT_PREP = 0
    # A transform body the planner marked as a user-defined function.
    UDF_BODY = 1
    # Assembling the transform's output back into blocks.
    OUTPUT_BUILD = 2
    # A transform body that isn't a UDF, such as a read or a write.
    OTHER = 3


@dataclass(frozen=True)
class TimedStep:
    """One ``Iterable -> Iterable`` link in a map task's pipeline.

    A task's whole transform is a flat list of these. ``apply`` is the work;
    everything else is metadata the summary groups by.

    ``bucket`` is ``None`` for a step that spans more than one phase, which is
    what a row transform gets when it is timed a stage at a time rather than a
    phase at a time. A phase no step carries is reported as "not measured"
    rather than as zero, so the ``None`` here is what produces the ``None``
    there.
    """

    label: str
    bucket: Optional[MapTransformPhase]
    stage_idx: int
    apply: Callable[[Iterable[Any]], Iterable[Any]]


class _TimedStep(Iterator[Any]):
    """Times one step, inclusive of everything upstream of it.

    Deliberately does not coordinate with the other steps: it starts a clock,
    pulls, and adds the elapsed time to its own slot. Because the chain is
    linear -- step ``k`` is only ever pulled by step ``k + 1`` -- all of a
    step's time lies inside its consumer's windows, so subtracting neighbouring
    totals at drain time recovers each step's own work. Doing that arithmetic
    once, over a list, is easier to follow than threading a shared cursor
    through every ``__next__``.
    """

    __slots__ = ("_apply", "_upstream", "_iter", "_totals", "_idx")

    def __init__(
        self,
        apply: Callable[[Iterable[Any]], Iterable[Any]],
        upstream: Iterable[Any],
        totals: List[float],
        idx: int,
    ):
        self._apply = apply
        self._upstream = upstream
        self._iter: Optional[Iterator[Any]] = None
        self._totals = totals
        self._idx = idx

    def __iter__(self) -> "_TimedStep":
        return self

    def __next__(self) -> Any:
        start = time.perf_counter()
        try:
            if self._iter is None:
                # Building the iterable is the step's work too, and for some
                # steps it is all of it: a write performs its whole upload here
                # and hands back a buffer. Deferring the call to the first pull
                # puts that inside the same window as the pulls, so an eagerly
                # consuming stage needs no special case.
                self._iter = iter(self._apply(self._upstream))
            return next(self._iter)
        finally:
            self._totals[self._idx] += time.perf_counter() - start


@dataclass(frozen=True)
class MapTransformPhaseTimes:
    """Seconds a task spent in its map transform chain.

    ``total_s`` is the whole chain, which is what Ray Data has always reported as
    "UDF time". The rest decompose it and sum back to it, saying where inside the
    chain the time went; they are all ``None`` when the chain measured only its
    total. Each is summed over every stage of a (possibly fused) chain, so a
    fused operator reports one figure per phase rather than one per stage.
    """

    total_s: float = 0.0
    # None, not zero, when the chain measured only its total: a consumer has to
    # be able to tell "not measured" from "measured as zero".
    input_prep_s: Optional[float] = None
    udf_body_s: Optional[float] = None
    output_build_s: Optional[float] = None
    other_s: Optional[float] = None


class TransformClock:
    """Per-task timing for one map transform chain.

    Must stay per task, not per :class:`MapTransformer`: an actor pool reuses
    one transformer for every task the actor runs, and
    ``max_concurrent_calls_per_actor > 1`` runs several at once, so a shared
    total would mix their timings and let one task's :meth:`drain` discard
    another's.

    Holds one inclusive total per step. :meth:`drain` turns those into each
    step's own time and groups them, so a step measures itself and nothing
    else while data is flowing.
    """

    __slots__ = ("inclusive", "_steps")

    def __init__(self) -> None:
        self._steps: List["TimedStep"] = []
        self.inclusive: List[float] = []

    def chain(self, steps: List["TimedStep"], blocks: Iterable[Any]) -> Iterable[Any]:
        """Build the timed pipeline over ``blocks``."""
        self._steps = steps
        self.inclusive = [0.0] * len(steps)
        data = blocks
        for idx, step in enumerate(steps):
            data = _TimedStep(step.apply, data, self.inclusive, idx)
        return data

    def _self_times(self) -> List[float]:
        """Each step's own time: its window less its upstream's.

        A step's window contains everything upstream of it, and step ``k`` is
        only ever pulled by step ``k + 1``, so neighbouring totals differ by
        exactly the step's own work. The clamp absorbs floating-point rounding
        on that subtraction; the difference is otherwise non-negative.
        """
        return [
            max(0.0, total - (self.inclusive[i - 1] if i else 0.0))
            for i, total in enumerate(self.inclusive)
        ]

    def drain(self) -> "MapTransformPhaseTimes":
        """Return the time accumulated since the last drain, and reset."""
        own = self._self_times()
        by_bucket: Dict[MapTransformPhase, float] = {}
        for step, seconds in zip(self._steps, own):
            if step.bucket is not None:
                by_bucket[step.bucket] = by_bucket.get(step.bucket, 0.0) + seconds

        # A step spanning all three phases carries no bucket, so if any step
        # carries one this chain was timed phase by phase. Deriving that from
        # the steps beats storing it: there is no flag to set inconsistently.
        if any(step.bucket is not None for step in self._steps):
            # Measured. A phase this chain happens not to run really did take
            # no time, so report zero -- `None` is reserved for "not measured",
            # which a consumer has to be able to tell apart.
            times = MapTransformPhaseTimes(
                total_s=sum(own),
                input_prep_s=by_bucket.get(MapTransformPhase.INPUT_PREP, 0.0),
                udf_body_s=by_bucket.get(MapTransformPhase.UDF_BODY, 0.0),
                output_build_s=by_bucket.get(MapTransformPhase.OUTPUT_BUILD, 0.0),
                other_s=by_bucket.get(MapTransformPhase.OTHER, 0.0),
            )
        else:
            times = MapTransformPhaseTimes(total_s=sum(own))
        # Zero in place. Every `_TimedStep` in the chain holds a reference to
        # this list, so rebinding it here would leave them adding to a list
        # this clock no longer reads -- and `_map_task` drains after every
        # output block, so only a task's first block would report any time.
        self.inclusive[:] = [0.0] * len(self.inclusive)
        return times


def _coalesce_stage(
    steps: List["TimedStep"], transform_fn: "MapTransformFn"
) -> "TimedStep":
    """Fold a stage's three steps into one, timed as a unit."""
    applies = [step.apply for step in steps]

    def apply(data: Iterable[Any]) -> Iterable[Any]:
        for fn in applies:
            data = fn(data)
        return data

    return TimedStep(
        label=f"{transform_fn!r} stage",
        # Spans input prep, body and output build, so it belongs to no single
        # phase -- which is what makes the phase figures report as "not
        # measured" for a chain timed this way.
        bucket=None,
        stage_idx=steps[0].stage_idx,
        apply=apply,
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
        *,
        init_fn: Optional[Callable[[], None]] = None,
        output_block_size_option_override: Optional[OutputBlockSizeOption] = None,
    ):
        """Initialize a :class:`MapTransformer`.

        Args:
            transform_fns: A list of `MapTransformFn`s that will be executed sequentially
                to transform data.
            init_fn: A function that will be called before transforming data.
                Used for the actor-based map operator.
            output_block_size_option_override: (Optional) Output block size configuration.
        """

        self._transform_fns: List[MapTransformFn] = []
        self._init_fn = init_fn if init_fn is not None else lambda: None
        self._output_block_size_option_override = output_block_size_option_override

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

    def get_timed_steps(
        self,
        ctx: TaskContext,
        report_custom_op_stats: CustomOpStatsReportFn,
        *,
        decomposed: bool,
    ) -> List["TimedStep"]:
        """This task's whole transform, as a flat list of timed steps.

        The answer to "what is getting timed?" is this list. It is ordinary
        data: printable, and assertable in a unit test.

        ``decomposed=False`` collapses each stage's three steps into one, which
        is how a row transform pays one timer per stage instead of three -- a
        rewrite of the list rather than a second code path.
        """
        steps: List[TimedStep] = []
        for stage_idx, transform_fn in enumerate(self._transform_fns):
            stage_steps = transform_fn.timed_steps(
                ctx, report_custom_op_stats, stage_idx
            )
            if not decomposed:
                stage_steps = [_coalesce_stage(stage_steps, transform_fn)]
            steps.extend(stage_steps)
        return steps

    def apply_transform(
        self,
        input_blocks: Iterable[Block],
        ctx: TaskContext,
        report_custom_op_stats: CustomOpStatsReportFn = _noop_report_custom_op_stats,
        *,
        clock: "TransformClock",
    ) -> Iterable[Block]:
        """Chain this task's timed steps over the input blocks.

        Args:
            input_blocks: The blocks to transform.
            ctx: The task context for this transform.
            report_custom_op_stats: Callback a producing transform calls to report
                its :class:`CustomOpStats`.
            clock: Where this task's timings accumulate. Keyword-only on
                purpose: a caller that silently skipped it would report zero
                rather than fail.

        Returns:
            An iterable of the transformed output blocks.
        """
        # NOTE: We only need to configure last transforming function to do
        #       appropriate block sizing
        last_transform = self._transform_fns[-1]

        if self.target_max_block_size_override is not None:
            last_transform.override_target_max_block_size(
                self.target_max_block_size_override
            )

        from ray.data.context import DataContext

        # A chain with no UDF in it -- a standalone read, write or projection --
        # has no user function to attribute time to, so it reports no UDF time,
        # as it always has.
        has_udf = any(fn._is_udf for fn in self._transform_fns)
        # Timing costs a Python frame per item. A batch transform yields whole
        # batches, so three timers per stage is noise; a row transform yields
        # rows, where it is not. Row chains get one timer per stage instead,
        # and `accurate_map_phase_timing` opts them into the full split.
        per_row = any(
            fn._input_type is MapTransformFnDataType.Row for fn in self._transform_fns
        )
        decomposed = not per_row or DataContext.get_current().accurate_map_phase_timing

        steps = self.get_timed_steps(ctx, report_custom_op_stats, decomposed=decomposed)

        if not has_udf:
            # Nothing to attribute, so no timer to pay for.
            data = input_blocks
            for step in steps:
                data = step.apply(data)
            return data

        return clock.chain(steps, input_blocks)

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


class RowMapTransformFn(MapTransformFn):
    """A rows-to-rows MapTransformFn."""

    def __init__(
        self,
        row_fn: MapTransformCallable[Row, Row],
        *,
        is_udf: bool = False,
        output_block_size_option: OutputBlockSizeOption,
        should_report_custom_op_stats: bool = False,
    ):
        super().__init__(
            row_fn,
            input_type=MapTransformFnDataType.Row,
            is_udf=is_udf,
            output_block_size_option=output_block_size_option,
            should_report_custom_op_stats=should_report_custom_op_stats,
        )

    def _pre_process(self, blocks: Iterable[Block]) -> Iterable[MapTransformFnData]:
        return _RowBasedIterator(blocks)

    def _post_process(self, results: Iterable[MapTransformFnData]) -> Iterable[Block]:
        return self._shape_blocks(results)

    def __repr__(self) -> str:
        return f"RowMapTransformFn({self._fn})"


def _peek_first_nonempty_block(
    blocks: Iterable[Block],
) -> Tuple[Optional[BlockAccessor], Iterable[Block]]:
    """Advance the iterator past leading empty blocks to find the first non-empty block,
    returning the corresponding accessor and a reconstructed iterator of all blocks.
    We must reconstruct the iterator because we consume blocks as we advance through the iterator."""
    blocks_iter = iter(blocks)
    consumed = []
    for block in blocks_iter:
        consumed.append(block)
        accessor = BlockAccessor.for_block(block)
        if accessor.num_rows() > 0 and accessor.size_bytes() > 0:
            return accessor, itertools.chain(consumed, blocks_iter)
    return None, iter(consumed)


def _compute_auto_batch_size(
    blocks: Iterable[Block],
    target_batch_size_bytes: int = _DEFAULT_BATCH_SIZE_BYTES,
) -> Tuple[Optional[int], Iterable[Block]]:
    """Peek at the first non-empty block to estimate the batch size to use for the
    'auto' batch_size option."""
    sample, blocks = _peek_first_nonempty_block(blocks)
    if sample is None:
        return None, blocks
    bytes_per_row = sample.size_bytes() / sample.num_rows()
    computed_batch_size = max(1, int(target_batch_size_bytes / bytes_per_row))
    return computed_batch_size, blocks


class BatchMapTransformFn(MapTransformFn):
    """A batch-to-batch MapTransformFn."""

    def __init__(
        self,
        batch_fn: MapTransformCallable[DataBatch, DataBatch],
        *,
        is_udf: bool = False,
        batch_size: Union[Optional[int], Literal["auto"]] = None,
        batch_format: Optional[BatchFormat] = None,
        zero_copy_batch: bool = True,
        output_block_size_option: Optional[OutputBlockSizeOption] = None,
        target_batch_size_bytes: int = _DEFAULT_BATCH_SIZE_BYTES,
        should_report_custom_op_stats: bool = False,
    ):
        super().__init__(
            batch_fn,
            input_type=MapTransformFnDataType.Batch,
            is_udf=is_udf,
            output_block_size_option=output_block_size_option,
            should_report_custom_op_stats=should_report_custom_op_stats,
        )

        self._batch_size = batch_size
        self._batch_format = batch_format
        self._zero_copy_batch = zero_copy_batch
        self._target_batch_size_bytes = target_batch_size_bytes

    def _pre_process(self, blocks: Iterable[Block]) -> Iterable[MapTransformFnData]:
        # TODO make batch-udf zero-copy by default
        if self._batch_size == "auto":
            batch_size, blocks = _compute_auto_batch_size(
                blocks, target_batch_size_bytes=self._target_batch_size_bytes
            )
        else:
            batch_size = self._batch_size
        ensure_copy = not self._zero_copy_batch and batch_size is not None
        return batch_blocks(
            blocks=iter(blocks),
            stats=None,
            batch_size=batch_size,
            batch_format=self._batch_format,
            ensure_copy=ensure_copy,
        )

    def _post_process(self, results: Iterable[MapTransformFnData]) -> Iterable[Block]:
        return self._shape_blocks(results)

    def __repr__(self) -> str:
        return f"BatchMapTransformFn({self._fn=}, {self._batch_format=}, {self._batch_size=}, {self._zero_copy_batch=})"


class BlockMapTransformFn(MapTransformFn):
    """A block-to-block MapTransformFn."""

    def __init__(
        self,
        block_fn: MapTransformCallable[Block, Block],
        *,
        is_udf: bool = False,
        disable_block_shaping: bool = False,
        output_block_size_option: Optional[OutputBlockSizeOption] = None,
        should_report_custom_op_stats: bool = False,
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
            should_report_custom_op_stats: If ``True``, ``block_fn`` accepts a third
                ``report_custom_op_stats`` callback argument and may report
                per-task :class:`CustomOpStats` to the driver.
        """

        super().__init__(
            block_fn,
            input_type=MapTransformFnDataType.Block,
            is_udf=is_udf,
            output_block_size_option=output_block_size_option,
            should_report_custom_op_stats=should_report_custom_op_stats,
        )

        self._disable_block_shaping = disable_block_shaping

    def _post_process(self, results: Iterable[MapTransformFnData]) -> Iterable[Block]:
        # Short-circuit for block transformations for which no
        # block-shaping is required
        if self._disable_block_shaping:
            return results

        return self._shape_blocks(results)

    def __repr__(self) -> str:
        return f"BlockMapTransformFn({self._fn=}, {self._output_block_size_option=})"


class _BlockShapingIterator(Iterator[Block]):
    """Iterator that shapes results into blocks using a buffer.

    Unlike a generator, local variables in __next__ go out of scope when the method
    returns, avoiding holding references to yielded values.
    """

    def __init__(
        self,
        results: Iterable[MapTransformFnData],
        input_type: MapTransformFnDataType,
        output_block_size_option: Optional[OutputBlockSizeOption],
    ):
        self._results_iter = iter(results)
        self._buffer = BlockOutputBuffer(output_block_size_option)
        self._finalized = False

        if input_type == MapTransformFnDataType.Block:
            self._append_buffer = self._buffer.add_block
        elif input_type == MapTransformFnDataType.Batch:
            self._append_buffer = self._buffer.add_batch
        else:
            assert input_type == MapTransformFnDataType.Row
            self._append_buffer = self._buffer.add

    def __iter__(self) -> "_BlockShapingIterator":
        return self

    def __next__(self) -> Block:
        while True:
            # First, yield any ready blocks from buffer
            if self._buffer.has_next():
                return self._buffer.next()

            # If finalized, no more data
            elif self._finalized:
                raise StopIteration

            try:
                # Fetch more results
                result = next(self._results_iter)
                self._append_buffer(result)
            except StopIteration:
                self._buffer.finalize()
                self._finalized = True


class _RowBasedIterator(Iterator[Row]):
    """Iterator that extracts rows from blocks.

    Unlike a generator, local variables in __next__ go out of scope when the method
    returns, avoiding holding references to yielded values.
    """

    def __init__(self, blocks: Iterable[Block]):
        self._blocks_iter = iter(blocks)
        self._cur_row_iter: Optional[Iterator[Row]] = None

    def __iter__(self) -> "_RowBasedIterator":
        return self

    def __next__(self) -> Row:
        while True:
            # Try to get next row from current block
            if self._cur_row_iter is not None:
                try:
                    return next(self._cur_row_iter)
                except StopIteration:
                    pass

            # Get iterator from the next block
            block = next(self._blocks_iter)

            self._cur_row_iter = BlockAccessor.for_block(block).iter_rows(
                public_row_format=True
            )
