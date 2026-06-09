import math
from dataclasses import dataclass
from typing import Any, Optional, Tuple

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.data.block_budget import BlockBudget, RowCount
from ray.data.context import MAX_SAFE_BLOCK_SIZE_FACTOR


@dataclass
class OutputBlockSizeOption:
    """How to shape the output blocks of an operator.

    Two independent layers:

    * ``target_max_block_size`` -- an intrinsic byte ceiling (memory safety),
      measured from the in-progress block builder. Applies to any input
      granularity (rows, batches, or blocks) and to every operator.
    * ``budgets`` -- zero or more :class:`~ray.data.block_budget.BlockBudget`
      packing constraints (row count, column sum, ...), measured per added
      *block*. Used by ``repartition``.

    Output blocks are cut at the tightest boundary across the byte ceiling and
    all budgets.
    """

    target_max_block_size: Optional[int] = None
    budgets: Tuple[BlockBudget, ...] = ()
    disable_block_shaping: bool = False

    def __post_init__(self):
        if (
            self.target_max_block_size is None
            and not self.budgets
            and not self.disable_block_shaping
        ):
            raise ValueError(
                "OutputBlockSizeOption must specify a byte size "
                "(`target_max_block_size`), at least one block budget, or "
                "`disable_block_shaping=True`."
            )

    @classmethod
    def of(
        cls,
        target_max_block_size: Optional[int] = None,
        target_num_rows_per_block: Optional[int] = None,
        *,
        block_budget: Optional[BlockBudget] = None,
        disable_block_shaping: bool = False,
    ) -> Optional["OutputBlockSizeOption"]:
        # TODO(Artur): Deprecate target_num_rows_per_block in favor of block_budget.
        budgets = []
        if target_num_rows_per_block is not None:
            budgets.append(RowCount(target_num_rows_per_block))
        if block_budget is not None:
            budgets.append(block_budget)

        if target_max_block_size is None and not budgets and not disable_block_shaping:
            # Nothing requested: the buffer won't yield incrementally, instead
            # producing just a single block from a single input block.
            return None
        return cls(
            target_max_block_size=target_max_block_size,
            budgets=tuple(budgets),
            disable_block_shaping=disable_block_shaping,
        )

    @property
    def target_num_rows_per_block(self) -> Optional[int]:
        """Backward compatibility: the row budget's limit, if a ``RowCount`` budget is set."""
        for budget in self.budgets:
            if isinstance(budget, RowCount):
                return budget.limit
        return None


class BlockOutputBuffer:
    """Generates output blocks of a target size from a stream of inputs.

    Turns a stream of items / batches / blocks into a stream of blocks shaped by
    the configured :class:`OutputBlockSizeOption`: an intrinsic byte ceiling
    (measured from the in-progress builder, any input granularity) and/or one or
    more :class:`~ray.data.block_budget.BlockBudget` packing constraints
    (measured per added block). The caller checks ``has_next()`` after each
    ``add*()`` call and calls ``next()`` to get the next block when it returns
    True. After all items are added, the caller must call ``finalize()`` and
    then check ``has_next()`` one last time.

    Examples:
        >>> from ray.data._internal.output_buffer import BlockOutputBuffer
        >>> udf = ... # doctest: +SKIP
        >>> generator = ... # doctest: +SKIP

        >>> output_block_size_option = OutputBlockSizeOption.of(target_max_block_size=500 * 1024 * 1024) # doctest: +SKIP
        >>> output = BlockOutputBuffer(output_block_size_option) # doctest: +SKIP
        >>> for item in generator(): # doctest: +SKIP
        ...     output.add(item) # doctest: +SKIP
        ...     if output.has_next(): # doctest: +SKIP
        ...         yield output.next() # doctest: +SKIP
        >>> output.finalize() # doctest: +SKIP
        >>> if output.has_next() # doctest: +SKIP
        ...     yield output.next() # doctest: +SKIP
    """

    def __init__(self, output_block_size_option: Optional[OutputBlockSizeOption]):
        self._output_block_size_option = output_block_size_option
        self._buffer = DelegatingBlockBuilder()
        self._finalized = False
        self._has_yielded_blocks = False
        # Running measure per budget, accumulated over added blocks (budgets are
        # measured at block granularity). The byte ceiling is read separately
        # from the builder, so it isn't tracked here.
        self._running = [0] * len(self._budgets())

    def _budgets(self) -> Tuple[BlockBudget, ...]:
        if self._output_block_size_option is None:
            return ()
        return self._output_block_size_option.budgets

    def _max_block_bytes(self) -> Optional[int]:
        if self._output_block_size_option is None:
            return None
        if self._output_block_size_option.disable_block_shaping:
            return None
        return self._output_block_size_option.target_max_block_size

    def _accumulate(self, block: Block) -> None:
        """Add ``block``'s measure to the running total for each budget."""
        accessor = BlockAccessor.for_block(block)
        for i, budget in enumerate(self._budgets()):
            self._running[i] += budget.measure(accessor)

    def add(self, item: Any) -> None:
        """Add a single row to this output buffer."""
        assert (
            not self._finalized
        ), f"{item} cannot add to a finalized BlockOutputBuffer."
        assert not self._budgets(), (
            "A block budget (from `ray.data.Dataset.repartition(block_budget=...)`) "
            "is measured per block and cannot be applied to row-by-row output."
        )
        self._buffer.add(item)

    def add_batch(self, batch: DataBatch) -> None:
        """Add a data batch to this output buffer."""
        assert (
            not self._finalized
        ), f"{batch} cannot add to a finalized BlockOutputBuffer."
        assert not self._budgets(), (
            "A block budget (from `ray.data.Dataset.repartition(block_budget=...)`) "
            "is measured per block and cannot be applied to batched output."
        )
        self._buffer.add_batch(batch)

    def add_block(self, block: Block) -> None:
        """Add a data block to this output buffer."""
        assert not self._finalized, "Cannot add to a finalized BlockOutputBuffer."
        self._buffer.add_block(block)
        self._accumulate(block)

    def finalize(self) -> None:
        """Must be called once all items have been added."""
        assert not self._finalized, "BlockOutputBuffer is already finalized."
        self._finalized = True

    def _exceeded_buffer_limit(self) -> bool:
        # Never cut an empty buffer (no-progress guard).
        if self._buffer.num_rows() == 0:
            return False
        # Different mechanisms because the builder tracks memory cheaply but no
        # other measure: read the byte ceiling straight from the builder, but tally
        # each budget per block ourselves (recomputing one from the builder on each
        # call would mean rebuilding the buffer -- quadratic).
        ceiling = self._max_block_bytes()
        if ceiling is not None and self._buffer.get_estimated_memory_usage() > ceiling:
            return True
        return any(
            self._running[i] > budget.limit for i, budget in enumerate(self._budgets())
        )

    def has_next(self) -> bool:
        """Returns true when a complete output block is produced."""

        # TODO remove emitting empty blocks
        if self._finalized:
            return not self._has_yielded_blocks or self._buffer.num_rows() > 0
        elif self._output_block_size_option is None:
            # NOTE: When block sizing is disabled, buffer won't be producing
            #       incrementally, until the whole sequence is ingested. This
            #       is required to align it with semantic of producing 1 block
            #       from 1 block of the input
            return False
        elif self._output_block_size_option.disable_block_shaping:
            # When block shaping is disabled, produce blocks immediately
            return self._buffer.num_rows() > 0

        return self._exceeded_buffer_limit()

    def _byte_boundary_row(self, accessor: BlockAccessor) -> Optional[int]:
        """Row to cut at to respect the byte ceiling, or None for no cut."""
        ceiling = self._max_block_bytes()
        num_rows = accessor.num_rows()
        if ceiling is None or num_rows == 0:
            return None
        # Only slice when meaningfully over the target, so the last block stays
        # at least half the target size.
        if accessor.size_bytes() < MAX_SAFE_BLOCK_SIZE_FACTOR * ceiling:
            return None
        num_bytes_per_row = accessor.size_bytes() / num_rows
        return max(1, math.ceil(ceiling / num_bytes_per_row))

    def _boundary_row(self, accessor: BlockAccessor) -> Optional[int]:
        """Row to cut the built block at: the tightest of byte ceiling + budgets."""
        if (
            self._output_block_size_option is None
            or self._output_block_size_option.disable_block_shaping
        ):
            return None
        candidates = []
        byte_cut = self._byte_boundary_row(accessor)
        if byte_cut is not None:
            candidates.append(byte_cut)
        candidates.extend(budget.boundary_row(accessor) for budget in self._budgets())
        return min(candidates) if candidates else None

    def next(self) -> Block:
        """Returns the next complete output block."""
        assert (
            self.has_next()
        ), "next() called when no output block is ready; check has_next() first."

        block = self._buffer.build()
        accessor = BlockAccessor.for_block(block)

        target_num_rows = self._boundary_row(accessor)
        block_remainder = None
        if target_num_rows is not None and target_num_rows < accessor.num_rows():
            block = accessor.slice(0, target_num_rows, copy=False)
            block_remainder = accessor.slice(
                target_num_rows, accessor.num_rows(), copy=False
            )

        self._buffer = DelegatingBlockBuilder()
        self._running = [0] * len(self._budgets())
        if block_remainder is not None:
            self._buffer.add_block(block_remainder)
            self._accumulate(block_remainder)

        self._has_yielded_blocks = True

        return block
