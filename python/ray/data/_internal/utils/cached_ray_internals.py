import ray
import ray._private.internal_api
from ray.data._internal.utils.cache import timed_cache


@timed_cache(ttl=1)
def get_local_ongoing_lineage_reconstruction_tasks():
    # ResourceManager.update_usages() calls extra_resource_usage() on
    # map operators. Unit tests exercise that path without ray.init(); lineage
    # reconstruction can only exist with a live worker, so treat offline as empty.
    if not ray.is_initialized():
        return []
    return ray._private.internal_api.get_local_ongoing_lineage_reconstruction_tasks()
