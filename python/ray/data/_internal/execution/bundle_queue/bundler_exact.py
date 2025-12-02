from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, Any, Deque, List, Optional, Tuple

from typing_extensions import override

from ray.data._internal.execution.bundle_queue import (
    BaseBundleQueue,
    SupportsRebundling,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle


"""Streaming repartition builds fixed-size outputs from a stream of inputs.

    We construct batches here to produce exactly sized outputs from arbitrary [start, end) slices across input blocks.
    The task builder submits a map task only after the total number of rows accumulated across pending blocks reaches
    target num rows (except during the final flush, which may emit a smaller tail block). This allows us to create
    target-sized batches without materializing entire large blocks on the driver.

    Detailed Implementation:
    1. When a new bundle arrives, buffer it in the pending list.
    2. Whenever the total number of rows in the pending bundles reaches the target row count, try to build a ready bundle.
    3. Determine the slice needed from the final bundle so the ready bundle holds an exact multiple of the target rows,
       and add the remaining bundle to the pending bundles for the next iteration.
    4. Submit that ready bundle to a remote map task; the task slices each block according to the slice metadata stored
       in the RefBundle (the bundle now contains n * target rows for n ≥ 1).
    5. We configured the `OutputBlockSizeOption.target_num_rows_per_block` to the target number of rows per block in
       plan_streaming_repartition_op so the output buffer further splits the n * target rows into n blocks of exactly
       the target size.
       Note: the output buffer only splits a bundle when its row count exceeds `target_rows * MAX_SAFE_ROWS_PER_BLOCK_FACTOR`
       (default 1.5). Because we split bundles into target-row blocks, `MAX_SAFE_ROWS_PER_BLOCK_FACTOR` must stay < 2 to
       output the target-row blocks.
    6. Once upstream input is exhausted, flush any leftover pending bundles and repeat steps 1-5 for the tail.
    7. The resulting blocks have lengths `[target, …, target, (total_rows % target)]`; ordering isn't guaranteed, but the
       remainder block should appear near the end.

"""


class ExactRebundleQueue(BaseBundleQueue, SupportsRebundling):
    """Incrementally builds task inputs to produce multiples of target-sized outputs."""

    def __init__(self, target_num_rows_per_block: int):
        super().__init__()
        assert (
            target_num_rows_per_block > 0
        ), "target_num_rows_per_block must be positive for streaming repartition."
        self._target_num_rows = target_num_rows_per_block
        self._pending_bundles: Deque[RefBundle] = deque()
        self._ready_bundles: Deque[RefBundle] = deque()
        self._total_pending_rows: int = 0
        self._consumed_input_bundles: List[RefBundle] = []

    def _merge_and_clear(self) -> RefBundle:

        from ray.data._internal.execution.interfaces import RefBundle

        for bundle in self._pending_bundles:
            self._on_dequeue(bundle)
        merged_bundle = RefBundle.merge_ref_bundles(list(self._pending_bundles))
        self._pending_bundles.clear()
        self._total_pending_rows = 0

        return merged_bundle

    def _try_build_ready_bundle(self, flush_remaining: bool = False):
        if self._total_pending_rows >= self._target_num_rows:
            rows_needed_from_last_bundle = (
                self._pending_bundles[-1].num_rows()
                - self._total_pending_rows % self._target_num_rows
            )
            assert rows_needed_from_last_bundle >= 0  # This will never be negative
            remaining_bundle = None

            if 0 < rows_needed_from_last_bundle < self._pending_bundles[-1].num_rows():
                # Need to slice the last bundle
                last_bundle = self._pending_bundles.pop()
                self._on_dequeue(last_bundle)  # Exit the original bundle

                sliced_bundle, remaining_bundle = last_bundle.slice(
                    rows_needed_from_last_bundle
                )

                self._pending_bundles.append(sliced_bundle)
                self._on_enqueue(sliced_bundle)  # Enter the sliced portion

            # Merge and clear all pending bundles
            merged_bundle = self._merge_and_clear()
            self._ready_bundles.append(merged_bundle)
            self._on_enqueue(merged_bundle)

            if remaining_bundle and remaining_bundle.num_rows() > 0:
                self._pending_bundles.append(remaining_bundle)
                self._on_enqueue(remaining_bundle)  # Enter the remainder
                self._total_pending_rows += remaining_bundle.num_rows()

        if flush_remaining and self._total_pending_rows > 0:
            merged_bundle = self._merge_and_clear()
            self._ready_bundles.append(merged_bundle)
            self._on_enqueue(merged_bundle)

    # TODO: remove this once we transition from op runtime metrics to
    # inner queue metrics.
    @override
    def get_next_with_original(self) -> Tuple[List[RefBundle], RefBundle]:
        consumed_input_bundles = self._consumed_input_bundles
        self._consumed_input_bundles = []
        ready_bundle = self._ready_bundles.popleft()
        self._on_dequeue(ready_bundle)
        return consumed_input_bundles, ready_bundle

    @override
    def add(self, bundle: RefBundle, **kwargs: Any):
        self._total_pending_rows += bundle.num_rows() or 0
        self._pending_bundles.append(bundle)
        self._on_enqueue(bundle)
        self._try_build_ready_bundle()
        self._consumed_input_bundles.append(bundle)

    @override
    def has_next(self) -> bool:
        return len(self._ready_bundles) > 0

    @override
    def get_next(
        self,
    ) -> RefBundle:
        if not self.has_next():
            raise ValueError("You can't pop from empty queue")
        bundle = self._ready_bundles.popleft()
        self._on_dequeue(bundle)
        return bundle

    @override
    def peek_next(self) -> Optional[RefBundle]:
        if not self.has_next():
            return None
        return self._ready_bundles[0]

    @override
    def done_adding_bundles(self, **kwargs: Any):
        if len(self._pending_bundles) > 0:
            self._try_build_ready_bundle(flush_remaining=True)

    @override
    def clear(self):
        self._reset_metrics()
        self._pending_bundles.clear()
        self._ready_bundles.clear()
        self._total_pending_rows = 0
        self._consumed_input_bundles.clear()


StreamingRepartitionRefBundler = ExactRebundleQueue
