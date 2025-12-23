from __future__ import annotations

import abc
from collections import deque
from typing import TYPE_CHECKING, Any, Deque, List, Optional

from typing_extensions import override

from ray.data._internal.execution.bundle_queue import (
    BaseBundleQueue,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle


class RebundlingStrategy(abc.ABC):
    """Base class for strategies describing how to rebundle queues."""

    pass

    @abc.abstractmethod
    def can_build_ready_bundle(self, num_pending_rows: int) -> bool:
        """Signifies whether we can build a ready bundle. A ready bundle is a bundle
        that will be returned from `get_next()` calls. Pending bundles merge into Ready bundles."""
        ...

    @abc.abstractmethod
    def rows_needed_from_last_pending_bundle(
        self, num_pending_rows: int, pending_bundle: RefBundle
    ) -> int:
        """Used to determine how to rebundle and slice an existing bundle.

        Args:
            num_pending_rows: The number of rows in a batch of pending bundles that will be merged to form
                a ready bundle
            pending_bundle: The last pending bundles in that batch ^. The term *last* means the bundle that caused
                `can_build_ready_bundle(num_pending_rows)` to be `True` for the first time.

        Returns:
            the # of rows needed from the last pending bundle.
        """
        ...


class EstimateSize(RebundlingStrategy):
    """Rebundles RefBundles to get them close to a particular number of rows."""

    def __init__(self, min_rows_per_bundle: Optional[int]):
        """Creates a strategy for combining bundles close to a particular row count.

        Args:
            min_rows_per_bundle: The target number of rows per bundle. Note that we
                bundle up to this target, but only exceed it if not doing so would
                result in an empty bundle. If None, this behaves like a normal queue.
        """

        assert (
            min_rows_per_bundle is None or min_rows_per_bundle >= 0
        ), "Min rows per bundle has to be non-negative"

        self._min_rows_per_bundle: Optional[int] = min_rows_per_bundle

    @override
    def can_build_ready_bundle(self, num_pending_rows: int) -> bool:
        return (
            self._min_rows_per_bundle is None
            or num_pending_rows >= self._min_rows_per_bundle
        )

    @override
    def rows_needed_from_last_pending_bundle(
        self, num_pending_rows: int, pending_bundle: RefBundle
    ) -> int:
        """Returns all the rows in the pending bundle, since we only care about an estimate"""
        return pending_bundle.num_rows() or 0


class ExactSize(RebundlingStrategy):
    def __init__(self, target_num_rows_per_block: int):
        assert (
            target_num_rows_per_block > 0
        ), "target_num_rows_per_block must be positive for streaming repartition."
        self._target_num_rows = target_num_rows_per_block

    @override
    def can_build_ready_bundle(self, num_pending_rows: int) -> bool:
        return num_pending_rows >= self._target_num_rows

    @override
    def rows_needed_from_last_pending_bundle(
        self, num_pending_rows: int, pending_bundle: RefBundle
    ) -> int:
        """Returns an exact # of rows from the last pending bundle."""
        extra_rows = num_pending_rows - self._target_num_rows
        assert extra_rows <= pending_bundle.num_rows()
        return pending_bundle.num_rows() - extra_rows


"""**For `ExactSize` strategy ONLY**

    Streaming repartition builds fixed-size outputs from a stream of inputs.

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
    6. Once upstream input is exhausted, flush any leftover pending bundles and repeat steps 1-5 for the tail.
    7. The resulting blocks have lengths `[target, …, target, (total_rows % target)]`; ordering isn't guaranteed, but the
       remainder block should appear near the end.

"""


class RebundleQueue(BaseBundleQueue):
    """Incrementally builds task inputs to produce multiples of target-sized outputs."""

    def __init__(self, strategy: RebundlingStrategy):
        super().__init__()

        self._strategy = strategy
        self._pending_bundles: Deque[RefBundle] = deque()
        self._ready_bundles: Deque[RefBundle] = deque()
        self._total_pending_rows: int = 0

    def _merge_bundles(self, pending_to_ready_bundles: List[RefBundle]):
        from ray.data._internal.execution.interfaces import RefBundle

        merged_bundle = RefBundle.merge_ref_bundles(pending_to_ready_bundles)
        self._ready_bundles.append(merged_bundle)
        self._on_enqueue(merged_bundle)

    def _try_build_ready_bundle(self, flush_remaining: bool = False):
        """Attempts to build a ready bundle from a list of pending bundles by:

        - Checking the threshold to build a ready bundle defined by `RebundlingStrategy`
        - Appropiately keeping track of queue metrics
        """

        if not (
            self._strategy.can_build_ready_bundle(self._total_pending_rows)
            or flush_remaining
        ):
            return

        pending_row_count_prefix_sum = 0
        pending_to_ready_bundles: List[RefBundle] = []

        while self._pending_bundles:

            pending_bundle = self._pending_bundles.popleft()
            self._on_dequeue(pending_bundle)  # Exit the original bundle
            self._total_pending_rows -= pending_bundle.num_rows() or 0
            pending_row_count_prefix_sum += pending_bundle.num_rows() or 0

            if self._strategy.can_build_ready_bundle(pending_row_count_prefix_sum):
                # We now know `pending_bundle` is the bundle that enabled us to
                # build a ready bundle. Therefore, we may need to slice the bundle.
                rows_needed = self._strategy.rows_needed_from_last_pending_bundle(
                    num_pending_rows=pending_row_count_prefix_sum,
                    pending_bundle=pending_bundle,
                )

                sliced_bundle, remaining_bundle = pending_bundle.slice(rows_needed)
                pending_to_ready_bundles.append(sliced_bundle)
                if remaining_bundle.num_rows():
                    self._pending_bundles.appendleft(remaining_bundle)
                    self._on_enqueue(remaining_bundle)  # Enter the remaining portion
                    self._total_pending_rows += remaining_bundle.num_rows() or 0

                self._merge_bundles(pending_to_ready_bundles)

                # reset the pending counts and continue converting pending to ready bundles.
                pending_row_count_prefix_sum = 0
                pending_to_ready_bundles: List[RefBundle] = []
                if not flush_remaining:
                    break
            else:
                # Entire pending_bundle complies with strategy. Add it to the list of bundles that
                # will be merged to form a ready bundle.
                pending_to_ready_bundles.append(pending_bundle)

        # If we're flushing and have leftover bundles, convert them to a ready bundle
        if flush_remaining and pending_to_ready_bundles:
            self._merge_bundles(pending_to_ready_bundles)

    @override
    def add(self, bundle: RefBundle, **kwargs: Any):
        self._total_pending_rows += bundle.num_rows() or 0
        self._pending_bundles.append(bundle)
        self._on_enqueue(bundle)
        self._try_build_ready_bundle()

    @override
    def has_next(self) -> bool:
        return len(self._ready_bundles) > 0

    @override
    def _get_next_inner(self) -> RefBundle:
        if not self.has_next():
            raise ValueError("You can't pop from empty queue")
        ready_bundle = self._ready_bundles.popleft()
        return ready_bundle

    @override
    def peek_next(self) -> Optional[RefBundle]:
        if not self.has_next():
            return None
        return self._ready_bundles[0]

    @override
    def finalize(self, **kwargs: Any):
        if len(self._pending_bundles) > 0:
            self._try_build_ready_bundle(flush_remaining=True)

    @override
    def clear(self):
        self._reset_metrics()
        self._pending_bundles.clear()
        self._ready_bundles.clear()
        self._total_pending_rows = 0
