from typing import List, Tuple

import ray
import ray._private.internal_api
from ray.core.generated import common_pb2
from ray.data._internal.utils.cache import timed_cache


@timed_cache(ttl=1)
def get_local_ongoing_lineage_reconstruction_tasks() -> List[
    Tuple[common_pb2.LineageReconstructionTask, int]
]:
    """Ongoing lineage reconstruction tasks submitted by this worker.

    The streaming executor polls this on every scheduling-loop iteration, so the
    result is cached to keep the hot loop cheap. The TTL is deliberately short:
    backpressure has to see reconstruction tasks appear (and disappear) promptly
    or it hands out budget Ray Core has already spent, which is the whole point
    of counting them. A longer window can hide a reconstruction that starts and
    finishes inside it, so the tasks are never charged at all.

    Returns an empty list when Ray isn't initialized. Operators report extra
    resource usage from `ResourceManager.update_usages()`, and unit tests
    exercise that path without `ray.init()`; lineage reconstruction can only
    exist with a live worker, so treat offline as "none ongoing".
    """
    if not ray.is_initialized():
        return []

    return ray._private.internal_api.get_local_ongoing_lineage_reconstruction_tasks()
