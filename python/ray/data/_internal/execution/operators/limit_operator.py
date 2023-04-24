from typing import Deque, Optional
from collections import deque
from ray.data._internal.stats import StatsDict
from ray.data._internal.execution.interfaces import (
    RefBundle,
    PhysicalOperator,
)
from ray.data._internal.split import _split_at_indices
from ray.data._internal.progress_bar import ProgressBar


class LimitOperator(PhysicalOperator):
    """Limit operator."""

    def __init__(
        self,
        limit: int,
        input_op: PhysicalOperator,
    ):
        self._limit = limit
        self._current_rows = 0
        self._name = f"LimitOperator[limit={limit}]"
        self._buffer: Deque[RefBundle] = deque()
        self._stats: StatsDict = {}
        self._num_outputs_total = input_op.num_outputs_total()
        if self._num_outputs_total is not None:
            self._num_outputs_total = min(self._num_outputs_total, limit)
        super().__init__(self._name, [input_op])

    def _limit_reached(self) -> bool:
        return self._current_rows >= self._limit

    def add_input(self, refs: RefBundle, input_index: int) -> None:
        assert not self.completed()
        assert input_index == 0, input_index
        if self._limit_reached():
            return
        input_rows = refs.num_rows()
        # TODO: What if input_rows is None?
        assert input_rows is not None, input_rows
        if input_rows + self._current_rows > self._limit:
            input_rows = self._limit - self._current_rows
            blocks_splits, metadata_splits = _split_at_indices(
                refs.blocks,
                [input_rows],
                owned_by_consumer=refs.owns_blocks,
            )
            refs = RefBundle(
                list(zip(blocks_splits[0], metadata_splits[0])),
                owns_blocks=refs.owns_blocks,
            )
        self._current_rows += input_rows
        self._buffer.append(refs)

    def has_next(self) -> bool:
        return len(self._buffer) > 0

    def get_next(self) -> RefBundle:
        return self._buffer.popleft()

    def get_stats(self) -> StatsDict:
        return self._stats

    def num_outputs_total(self) -> Optional[int]:
        if self._limit_reached():
            return self._limit
        else:
            return self._num_outputs_total
