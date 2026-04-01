from collections import deque
from typing import Deque, List, Tuple

from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.execution.operators.map_operator import BaseRefBundler


class DistributedShuffleRefBundler(BaseRefBundler):
    """Buffers input RefBundles until their total size exceeds the shuffle window
    (in bytes), then emits them as a single merged bundle to be shuffled."""

    def __init__(self, shuffle_window_bytes: int):
        assert (
            shuffle_window_bytes > 0
        ), "shuffle_window_bytes must be positive for distributed shuffle."
        self._shuffle_window_bytes = shuffle_window_bytes
        self._pending_bundles: Deque[RefBundle] = deque()
        self._ready_bundles: Deque[RefBundle] = deque()
        self._consumed_input_bundles: List[RefBundle] = []
        self._total_pending_bytes = 0

    def _try_build_ready_bundle(self, flush_remaining: bool = False):
        while self._total_pending_bytes >= self._shuffle_window_bytes:
            pending_bundles = list(self._pending_bundles)
            self._ready_bundles.append(RefBundle.merge_ref_bundles(pending_bundles))
            self._pending_bundles.clear()
            self._total_pending_bytes = 0

        if flush_remaining and len(self._pending_bundles) > 0:
            self._ready_bundles.append(
                RefBundle.merge_ref_bundles(self._pending_bundles)
            )
            self._pending_bundles.clear()
            self._total_pending_bytes = 0

    def add_bundle(self, ref_bundle: RefBundle):
        self._total_pending_bytes += ref_bundle.size_bytes()
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
