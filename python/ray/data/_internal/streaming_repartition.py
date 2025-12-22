from collections import deque
from typing import Deque, List, Tuple

from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.execution.operators.map_operator import BaseRefBundler

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
       in the RefBundle (the bundle now contains n × target rows for n ≥ 1).
    5. We configured the `OutputBlockSizeOption.target_num_rows_per_block` to the target number of rows per block in
       plan_streaming_repartition_op so the output buffer further splits the n × target rows into n blocks of exactly
       the target size.
    6. Once upstream input is exhausted, flush any leftover pending bundles and repeat steps 1‑5 for the tail.
    7. The resulting blocks have lengths `[target, …, target, (total_rows % target)]`; ordering isn’t guaranteed, but the
       remainder block should appear near the end.

"""


class StreamingRepartitionRefBundler(BaseRefBundler):
    """Incrementally builds task inputs to produce multiples of target-sized outputs."""

    def __init__(self, target_num_rows_per_block: int):
        assert (
            target_num_rows_per_block > 0
        ), "target_num_rows_per_block must be positive for streaming repartition."
        self._target_num_rows = target_num_rows_per_block
        self._pending_bundles: Deque[RefBundle] = deque()
        self._ready_bundles: Deque[RefBundle] = deque()
        self._consumed_input_bundles: List[RefBundle] = []
        self._total_pending_rows = 0

    def _try_build_ready_bundle(self, flush_remaining: bool = False):
        if self._total_pending_rows >= self._target_num_rows:
            rows_needed_from_last_bundle = (
                self._pending_bundles[-1].num_rows()
                - self._total_pending_rows % self._target_num_rows
            )
            assert rows_needed_from_last_bundle >= 0  # This will never be negative
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
        if flush_remaining and self._total_pending_rows > 0:
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
