from collections import deque
from typing import Callable, List, Optional

from ray.data._internal.stats import StatsDict
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    RefBundle,
    PhysicalOperator,
)


class OutputSplitter(PhysicalOperator):
    """An operator that splits the given data into `n` shards.

    The output bundles of this operator will have a `bundle.shard` attribute set to an
    integer from [0..n-1]. This operator tries to divide the rows evenly across shards.

    If the `equal` option is set, the operator will furthermore guarantee an exact
    split of rows across shards, truncating the Dataset as needed.
    """

    def __init__(
        self,
        input_op: PhysicalOperator,
        n: int,
        equal: bool,  # TODO: implement output locality
    ):
        super().__init__(f"split({n}, equal={equal})", [input_op])
        self._n = n
        self._equal = equal
        # Buffer of bundles not yet assigned to shards.
        self._buffer = []
        # The outputted bundles with shard attribute set.
        self._output_queue: List[RefBundle] = []
        # The number of rows output to each shard so far.
        self._num_output: List[int] = [0 for _ in range(n)]

    def has_next(self) -> bool:
        return len(self._output_queue) > 0

    def get_next(self) -> RefBundle:
        return self._output_queue.pop()

    def get_stats(self) -> StatsDict:
        stats = {}
        for i, num in enumerate(self._num_output):
            stats[f"num_output_{i}"] = num
        return stats

    def add_input(self, bundle, input_index) -> None:
        if bundle.num_rows() is None:
            raise ValueError("OutputSplitter requires bundles with known row count")
        self._buffer.append(bundle)
        self._dispatch_bundles()

    def inputs_done(self) -> None:
        if self._equal:
            # TODO: launch tasks to do the split and block until completion.
            raise NotImplementedError
        else:
            assert not self._buffer

    def progress_str(self) -> str:
        if self._equal:
            return f"{len(self._buffer)} buffered"
        else:
            assert not self._buffer
            return ""

    def _dispatch_bundles(self) -> None:
        """Dispatch all dispatchable bundles from the internal buffer.

        This may not dispatch all bundles when equal=True.
        """
        while self._buffer:
            target_index = self._select_output_index()
            target_bundle = self._pop_bundle_to_dispatch(target_index)
            if self._can_safely_dispatch(target_index, target_bundle.num_rows()):
                target_bundle.shard = target_index
                self._num_sent[i] += target_bundle.num_rows()
                self._output_queue.append(target_bundle)
            else:
                # Put it back and abort.
                self._buffer.insert(0, target_bundle)
                break

    def _select_output_index(self) -> int:
        # Pick the consumer we'd like to dispatch for.
        i, _ = min(enumerate(self._num_output), key=lambda t: t[1])
        return i

    def _pop_bundle_to_dispatch(self, target_index: int) -> RefBundle:
        # TODO implement locality aware bundle selection.
        return self._buffer.pop(0)

    def _can_safely_dispatch(self, target_index: int, nrow: int) -> bool:
        if not self._equal:
            return True
        result = self._num_output.copy()
        result[i] += nrow
        # Calculate the new number of rows that we'd need to equalize the row
        # distribution after the bundle dispatch.
        max_r = max(result)
        buffer_requirement = sum([max_r - n for n in result])
        buffer_size = sum(b.num_rows() for b in self._buffer)
        return buffer_size >= buffer_requirement
