from typing import List, Tuple

import ray
import ray._private.internal_api
from ray.core.generated import common_pb2
from ray.data._internal.utils.cache import timed_cache


@timed_cache(ttl=60)
def get_local_ongoing_lineage_reconstruction_tasks() -> List[
    Tuple[common_pb2.LineageReconstructionTask, int]
]:
    """Ongoing lineage reconstruction tasks submitted by this worker.

    The streaming executor polls this on every scheduling-loop iteration, which
    is far more often than the value can meaningfully change, hence the TTL.
    Tests that need to observe a change within their own lifetime bypass the
    cache with the `disable_timed_cache_fixture` fixture.

    Returns an empty list when Ray isn't initialized. Operators report extra
    resource usage from `ResourceManager.update_usages()`, and unit tests
    exercise that path without `ray.init()`; lineage reconstruction can only
    exist with a live worker, so treat offline as "none ongoing".
    """
    if not ray.is_initialized():
        return []

    return ray._private.internal_api.get_local_ongoing_lineage_reconstruction_tasks()
