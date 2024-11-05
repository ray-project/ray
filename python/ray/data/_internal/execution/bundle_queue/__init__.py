from .bundle_queue import BundleQueue
from .fifo_bundle_queue import FIFOBundleQueue
from ray.data.context import DataContext


def create_bundle_queue() -> BundleQueue:
    from ray._private.ray_constants import env_bool

    if (
        env_bool("RAY_DATA_ENABLE_LOCATION_AWARE_BUNDLE_QUEUES", True)
        # Location aware bundle queues change the order of inputs to prioritize bundles
        # that exist on object store memory. This breaks order preservation, so we
        # disable if `preserve_order` is set.
        and not DataContext.get_current().execution_options.preserve_order
    ):
        from ray.anyscale.data._internal.location_aware_bundle_queue import (
            LocationAwareBundleQueue,
        )

        return LocationAwareBundleQueue()
    else:
        return FIFOBundleQueue()


__all__ = ["BundleQueue", "create_bundle_queue"]
