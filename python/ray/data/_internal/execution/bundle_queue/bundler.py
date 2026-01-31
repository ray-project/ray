from __future__ import annotations

import abc
from collections import deque
from typing import TYPE_CHECKING, Any, Deque, List, Optional, Tuple

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
        self, total_pending_rows: int, last_pending_bundle: RefBundle
    ) -> int:
        """Used to determine how to rebundle and slice an existing bundle.

        Args:
            total_pending_rows: The number of rows in a batch of pending bundles that will be merged to form
                a ready bundle, including the last_pending_bundle.
            last_pending_bundle: The last pending bundles in that batch ^. The term *last* means the bundle that caused
                `can_build_ready_bundle(num_pending_rows)` to be `True` for the first time.

        Returns:
            The # of rows needed from the last pending bundle. This should be > 0, unless bundle.num_rows() is None.
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
    def can_build_ready_bundle(self, total_pending_rows: int) -> bool:
        return total_pending_rows > 0 and (
            self._min_rows_per_bundle is None
            or total_pending_rows >= self._min_rows_per_bundle
        )

    @override
    def rows_needed_from_last_pending_bundle(
        self, total_pending_rows: int, last_pending_bundle: RefBundle
    ) -> int:
        """Returns all the rows in the pending bundle, since we only care about an estimate"""
        return last_pending_bundle.num_rows() or 0


class ExactMultipleSize(RebundlingStrategy):
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
        self, total_pending_rows: int, last_pending_bundle: RefBundle
    ) -> int:
        """Returns an exact MULTIPLE of target_num_rows from the last pending bundle."""
        pending_rows = last_pending_bundle.num_rows() or 0
        assert total_pending_rows - pending_rows < self._target_num_rows, (
            f"Total pending rows={total_pending_rows} should be less than target_num_rows={self._target_num_rows}, "
            "because last_pending_bundle should trigger building ready bundles"
        )
        extra_rows = total_pending_rows % self._target_num_rows
        assert extra_rows < pending_rows
        return pending_rows - extra_rows


"""**For `ExactMultipleSize` strategy ONLY**

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

        self._curr_consumed_bundles: List[RefBundle] = []
        # The original bundles that formed a ready bundle
        self._consumed_bundles_list: Deque[List[RefBundle]] = deque()
        self._total_pending_rows: int = 0

    def _merge_bundles(self):
        """Combine *ALL* pending_bundles into a single, ready bundle."""
        from ray.data._internal.execution.interfaces import RefBundle

        merged_bundle = RefBundle.merge_ref_bundles(self._pending_bundles)
        self._ready_bundles.append(merged_bundle)
        self._on_enqueue_bundle(merged_bundle)

        # Clear the pending queue since all bundles have been processed
        for bundle in self._pending_bundles:
            self._on_dequeue_bundle(bundle)
        self._pending_bundles.clear()
        self._total_pending_rows = 0

    def _try_build_ready_bundle(self, flush_remaining: bool = False) -> bool:
        """Attempts to build a ready bundle from a list of pending bundles by:

        - Checking the threshold to build a ready bundle defined by `RebundlingStrategy`
        - Appropiately keeping track of queue metrics

        Returns `True` if ready bundle built, otherwise `False`
        """

        built_ready_bundle: bool = False
        if self._pending_bundles and self._strategy.can_build_ready_bundle(
            self._total_pending_rows
        ):
            last_pending_bundle = self._pending_bundles.pop()

            # We now know `pending_bundle` is the bundle that enabled us to
            # build a ready bundle. Therefore, we may need to slice the bundle.
            rows_needed = self._strategy.rows_needed_from_last_pending_bundle(
                total_pending_rows=self._total_pending_rows,
                last_pending_bundle=last_pending_bundle,
            )
            assert rows_needed > 0, (
                "A refbundle has zero row-count but triggered building a ready bundle"
                "This is a bug in the Ray Data code."
            )
            remaining_bundle: Optional[RefBundle] = None
            if rows_needed < last_pending_bundle.num_rows():
                sliced_bundle, remaining_bundle = last_pending_bundle.slice(rows_needed)
                # The original bundle was enqueued in add(). We need to dequeue it
                # and enqueue the sliced portion, since _merge_bundles will dequeue
                # sliced_bundle (which has different metrics than the original).
                self._on_dequeue_bundle(last_pending_bundle)
                self._on_enqueue_bundle(sliced_bundle)
                self._pending_bundles.append(sliced_bundle)
            else:
                assert rows_needed == last_pending_bundle.num_rows()
                self._pending_bundles.append(last_pending_bundle)

            self._merge_bundles()
            built_ready_bundle = True

            if remaining_bundle is not None:
                # Add back remaining sliced bundle that was not included to build
                # a ready bundle.
                self._pending_bundles.appendleft(remaining_bundle)
                self._total_pending_rows += remaining_bundle.num_rows() or 0
                self._on_enqueue_bundle(remaining_bundle)

        # If we're flushing and have leftover bundles, convert them to a ready bundle
        if flush_remaining and self._pending_bundles:
            self._merge_bundles()
            built_ready_bundle = True

        return built_ready_bundle

    @override
    def add(self, bundle: RefBundle, **kwargs: Any):
        self._total_pending_rows += bundle.num_rows() or 0
        self._pending_bundles.append(bundle)
        self._on_enqueue_bundle(bundle)
        self._curr_consumed_bundles.append(bundle)
        if self._try_build_ready_bundle():
            self._consumed_bundles_list.append(self._curr_consumed_bundles)
            self._curr_consumed_bundles = []

    @override
    def has_next(self) -> bool:
        return len(self._ready_bundles) > 0

    @override
    def _get_next_inner(self) -> RefBundle:
        if not self.has_next():
            raise ValueError("You can't pop from empty queue")
        ready_bundle = self._ready_bundles.popleft()
        # discard the original bundle
        self._consumed_bundles_list.popleft()
        return ready_bundle

    def get_next_with_original(self) -> Tuple[RefBundle, List[RefBundle]]:
        if not self.has_next():
            raise ValueError("You can't pop from empty queue")
        ready_bundle = self._ready_bundles.popleft()
        self._on_dequeue_bundle(ready_bundle)
        consumed_bundle = self._consumed_bundles_list.popleft()
        return ready_bundle, consumed_bundle

    @override
    def peek_next(self) -> Optional[RefBundle]:
        if not self.has_next():
            return None
        return self._ready_bundles[0]

    @override
    def finalize(self, **kwargs: Any):
        if len(self._pending_bundles) > 0:
            assert self._try_build_ready_bundle(flush_remaining=True)
            self._consumed_bundles_list.append(self._curr_consumed_bundles)
            self._curr_consumed_bundles = []

    @override
    def clear(self):
        self._reset_metrics()
        self._pending_bundles.clear()
        self._ready_bundles.clear()
        self._curr_consumed_bundles.clear()
        self._consumed_bundles_list.clear()
        self._total_pending_rows = 0
