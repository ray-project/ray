import logging
import os
import time
from typing import Any, Dict, Optional

import ray
from ray._private import ray_constants
from ray._private.resource_spec import NODE_ID_PREFIX
from ray.tune.resources import Resources

logger = logging.getLogger(__name__)

TUNE_STATE_REFRESH_PERIOD = 10  # Refresh resources every 10 s


def _to_gb(n_bytes):
    return round(n_bytes / (1024 ** 3), 2)


class _ResourceUpdater:
    """Periodic Resource updater for Tune.

    Initially, all resources are set to 0. The updater will try to update resources
    when (1) init ResourceUpdater (2) call "update_avail_resources", "num_cpus"
    or "num_gpus".

    The update takes effect when (1) Ray is initialized (2) the interval between
    this and last update is larger than "refresh_period"
    """

    def __init__(self, refresh_period: Optional[float] = None):
        self._avail_resources = Resources(cpu=0, gpu=0)

        if refresh_period is None:
            refresh_period = float(
                os.environ.get("TUNE_STATE_REFRESH_PERIOD", TUNE_STATE_REFRESH_PERIOD)
            )
        self._refresh_period = refresh_period
        self._last_resource_refresh = float("-inf")
        self.update_avail_resources()

    def update_avail_resources(self, num_retries=5):
        if not ray.is_initialized():
            return
        if time.time() - self._last_resource_refresh < self._refresh_period:
            return
        logger.debug("Checking Ray cluster resources.")
        resources = None
        for i in range(num_retries):
            if i > 0:
                logger.warning(
                    f"Cluster resources not detected or are 0. Attempt #{i + 1}...",
                )
                time.sleep(0.5)
            resources = ray.cluster_resources()
            if resources:
                break

        if not resources:
            # NOTE: This hides the possibility that Ray may be waiting for
            # clients to connect.
            resources.setdefault("CPU", 0)
            resources.setdefault("GPU", 0)
            logger.warning(
                "Cluster resources cannot be detected or are 0. "
                "You can resume this experiment by passing in `resume=True` to `run`."
            )

        resources = resources.copy()
        num_cpus = resources.pop("CPU", 0)
        num_gpus = resources.pop("GPU", 0)
        memory = ray_constants.from_memory_units(resources.pop("memory", 0))
        object_store_memory = ray_constants.from_memory_units(
            resources.pop("object_store_memory", 0)
        )
        custom_resources = resources

        self._avail_resources = Resources(
            int(num_cpus),
            int(num_gpus),
            memory=int(memory),
            object_store_memory=int(object_store_memory),
            custom_resources=custom_resources,
        )
        self._last_resource_refresh = time.time()

    def debug_string(self, total_resources: Dict[str, Any]) -> str:
        """Returns a human readable message for printing to the console."""
        if self._last_resource_refresh > 0:
            status = (
                "Resources requested: {}/{} CPUs, {}/{} GPUs, "
                "{}/{} GiB heap, {}/{} GiB objects".format(
                    total_resources.pop("CPU", 0),
                    self._avail_resources.cpu,
                    total_resources.pop("GPU", 0),
                    self._avail_resources.gpu,
                    _to_gb(total_resources.pop("memory", 0.0)),
                    _to_gb(self._avail_resources.memory),
                    _to_gb(total_resources.pop("object_store_memory", 0.0)),
                    _to_gb(self._avail_resources.object_store_memory),
                )
            )
            customs = ", ".join(
                [
                    "{}/{} {}".format(
                        total_resources.get(name, 0.0),
                        self._avail_resources.get_res_total(name),
                        name,
                    )
                    for name in self._avail_resources.custom_resources
                    if not name.startswith(NODE_ID_PREFIX)
                    and (total_resources.get(name, 0.0) > 0 or "_group_" not in name)
                ]
            )
            if customs:
                status += f" ({customs})"
            return status
        else:
            return "Resources requested: ?"

    def get_num_cpus(self) -> int:
        self.update_avail_resources()
        return self._avail_resources.cpu

    def get_num_gpus(self) -> int:
        self.update_avail_resources()
        return self._avail_resources.gpu

    def __reduce__(self):
        # Do not need to serialize resources, because we can always
        # update it again. This also prevents keeping outdated resources
        # when deserialized.
        return _ResourceUpdater, (self._refresh_period,)
