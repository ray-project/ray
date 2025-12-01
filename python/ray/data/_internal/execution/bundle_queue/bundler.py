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


class RebundleQueue(BaseBundleQueue, SupportsRebundling):
    """Rebundles RefBundles to get them close to a particular number of rows."""

    def __init__(self, min_rows_per_bundle: Optional[int]):
        """Creates a BlockRefBundler.

        Args:
            min_rows_per_bundle: The target number of rows per bundle. Note that we
                bundle up to this target, but only exceed it if not doing so would
                result in an empty bundle. If None, this behaves like a normal queue.
        """

        super().__init__()

        assert (
            min_rows_per_bundle is None or min_rows_per_bundle >= 0
        ), "Min rows per bundle has to be non-negative"

        self._min_rows_per_bundle: Optional[int] = min_rows_per_bundle
        self._bundle_buffer: Deque[RefBundle] = deque([])
        self._finalized: bool = False

    @override
    def add(self, bundle: RefBundle, **kwargs: Any) -> None:
        self._bundle_buffer.append(bundle)
        self._on_enter(bundle)

    @override
    def get_next(self) -> RefBundle:
        bundle = self.peek_next()
        if bundle is None:
            raise ValueError(
                f"Popping from empty {self.__class__.__name__} is prohibited"
            )

        self._on_exit(bundle)
        self._bundle_buffer.popleft()
        return bundle

    @override
    def peek_next(self) -> Optional[RefBundle]:
        pack = self._peek_next_inner()
        if pack is None:
            return None
        _, merged_bundle = pack
        return merged_bundle

    @override
    def has_next(self) -> bool:
        return self._bundle_buffer and (
            self._min_rows_per_bundle is None
            or self._num_rows >= self._min_rows_per_bundle
            or (self._finalized and self._num_rows >= 0)
        )

    def _peek_next_inner(self) -> Optional[Tuple[List[RefBundle], RefBundle]]:
        if not self.has_next():
            return None

        if self._min_rows_per_bundle is None:
            # Short-circuit if no bundle row target was defined.
            bundle = self._bundle_buffer[0]
            return [bundle], bundle

        remainder = []
        output_buffer = []
        output_buffer_size = 0

        for idx, bundle in enumerate(self._bundle_buffer):
            bundle_size = _get_bundle_size(bundle)

            # Add bundle to the output buffer so long as either
            #   - Output buffer size is still 0
            #   - Output buffer doesn't exceed the `_min_rows_per_bundle` threshold
            if (
                output_buffer_size < self._min_rows_per_bundle
                or output_buffer_size == 0
            ):
                output_buffer.append(bundle)
                output_buffer_size += bundle_size
                self._on_exit(bundle)
            else:
                remainder = self._bundle_buffer[idx:]
                break

        self._bundle_buffer = deque(remainder)

        # Merge the bundles together and append them to the front of the queue
        from ray.data._internal.execution.interfaces import RefBundle

        merged_bundle = RefBundle.merge_ref_bundles(output_buffer)
        self._on_enter(merged_bundle)
        self._bundle_buffer.appendleft(merged_bundle)
        return output_buffer, merged_bundle

    # TODO: remove this once we transition from op runtime metrics to
    # inner queue metrics.
    @override
    def get_next_with_original(self) -> Tuple[List[RefBundle], RefBundle]:
        pack = self._peek_next_inner()
        if pack is None:
            raise ValueError(
                f"Popping from empty {self.__class__.__name__} is prohibited"
            )

        self._on_exit(pack[1])
        self._bundle_buffer.popleft()
        return pack

    @override
    def done_adding_bundles(self, **kwargs: Any):
        self._finalized = True

    @override
    def clear(self):
        self._bundle_buffer.clear()
        self._finalized = False


BlockRefBundler = RebundleQueue


def _get_bundle_size(bundle: RefBundle):
    return bundle.num_rows() if bundle.num_rows() is not None else float("inf")
