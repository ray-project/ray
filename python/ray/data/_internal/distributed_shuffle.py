from collections import deque
from typing import Deque, List, Tuple

from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.execution.operators.map_operator import BaseRefBundler

"""Distributed local shuffle builds shuffled outputs from a stream of inputs.

    This operator buffers incoming blocks until the total number of rows reaches a
    configurable window size, then emits the buffered blocks as a single bundle to a
    map task that shuffles the rows. This provides a streaming shuffle that doesn't
    require materializing the entire dataset, at the cost of only shuffling within a
    window rather than globally.

    Detailed Implementation:
    1. When a new bundle arrives, buffer it in the pending list.
    2. Whenever the total number of rows in the pending bundles reaches the window size,
       build a ready bundle from the pending bundles.
    3. Determine the slice needed from the final bundle so the ready bundle holds exactly
       the window size rows, and keep the remainder for the next window.
    4. Submit that ready bundle to a remote map task; the task concatenates all blocks,
       shuffles the rows randomly, and outputs the shuffled block(s).
    5. Once upstream input is exhausted, flush any leftover pending bundles as a final
       (possibly smaller) shuffle window.
"""


class DistributedShuffleRefBundler(BaseRefBundler):
    """Buffers input RefBundles until they reach the shuffle window size,
    then emits them as a single bundle to be shuffled by a map task."""

    def __init__(self, shuffle_window_size: int):
        assert (
            shuffle_window_size > 0
        ), "shuffle_window_size must be positive for distributed shuffle."
        self._shuffle_window_size = shuffle_window_size
        self._pending_bundles: Deque[RefBundle] = deque()
        self._ready_bundles: Deque[RefBundle] = deque()
        self._consumed_input_bundles: List[RefBundle] = []
        self._total_pending_rows = 0

    def _try_build_ready_bundle(self, flush_remaining: bool = False):
        while self._total_pending_rows >= self._shuffle_window_size:
            rows_needed_from_last_bundle = (
                self._pending_bundles[-1].num_rows()
                - self._total_pending_rows % self._shuffle_window_size
            )
            # When total_pending_rows is an exact multiple of window size,
            # rows_needed_from_last_bundle equals the last bundle's full row count,
            # so we don't need to slice.
            if rows_needed_from_last_bundle == 0:
                rows_needed_from_last_bundle = self._pending_bundles[-1].num_rows()

            pending_bundles = list(self._pending_bundles)
            remaining_bundle = None
            if (
                rows_needed_from_last_bundle > 0
                and rows_needed_from_last_bundle < pending_bundles[-1].num_rows()
            ):
                last_bundle = pending_bundles.pop()
                sliced_bundle, remaining_bundle = last_bundle.slice(
                    rows_needed_from_last_bundle
                )
                pending_bundles.append(sliced_bundle)
            self._ready_bundles.append(RefBundle.merge_ref_bundles(pending_bundles))
            self._pending_bundles.clear()
            self._total_pending_rows = 0
            if remaining_bundle and remaining_bundle.num_rows() > 0:
                self._pending_bundles.append(remaining_bundle)
                self._total_pending_rows += remaining_bundle.num_rows()

        if flush_remaining and len(self._pending_bundles) > 0:
            self._ready_bundles.append(
                RefBundle.merge_ref_bundles(self._pending_bundles)
            )
            self._pending_bundles.clear()
            self._total_pending_rows = 0

    def add_bundle(self, ref_bundle: RefBundle):
        self._total_pending_rows += ref_bundle.num_rows()
        self._pending_bundles.append(ref_bundle)
        self._try_build_ready_bundle()
        self._consumed_input_bundles.append(ref_bundle)

    def has_bundle(self) -> bool:
        return len(self._ready_bundles) > 0

    def get_next_bundle(
        self,
    ) -> Tuple[List[RefBundle], RefBundle]:
        consumed_input_bundles = self._consumed_input_bundles
        self._consumed_input_bundles = []
        return consumed_input_bundles, self._ready_bundles.popleft()

    def done_adding_bundles(self):
        if len(self._pending_bundles) > 0:
            self._try_build_ready_bundle(flush_remaining=True)

    def num_blocks(self):
        return sum(len(bundle) for bundle in self._pending_bundles) + sum(
            len(bundle) for bundle in self._ready_bundles
        )

    def size_bytes(self) -> int:
        return sum(bundle.size_bytes() for bundle in self._pending_bundles) + sum(
            bundle.size_bytes() for bundle in self._ready_bundles
        )
