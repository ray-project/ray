from .bundle_queue import BundleQueue
from .fifo_bundle_queue import FIFOBundleQueue


def create_bundle_queue() -> BundleQueue:
    return FIFOBundleQueue()


__all__ = ["BundleQueue", "create_bundle_queue"]
