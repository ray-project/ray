import logging
import os
import time
from collections import namedtuple
from numbers import Number
from typing import Any, Dict, Optional

import ray
from ray._private.resource_spec import NODE_ID_PREFIX

logger = logging.getLogger(__name__)

TUNE_STATE_REFRESH_PERIOD = 10  # Refresh resources every 10 s


def _to_gb(n_bytes):
    return round(n_bytes / (1024**3), 2)


class _Resources(
    namedtuple(
        "_Resources",
        [
            "cpu",
            "gpu",
            "memory",
            "object_store_memory",
            "extra_cpu",
            "extra_gpu",
            "extra_memory",
            "extra_object_store_memory",
            "custom_resources",
            "extra_custom_resources",
            "has_placement_group",
        ],
    )
):
    """Ray resources required to schedule a trial.

    Parameters:
        cpu: Number of CPUs to allocate to the trial.
        gpu: Number of GPUs to allocate to the trial.
        memory: Memory to reserve for the trial.
        object_store_memory: Object store memory to reserve.
        extra_cpu: Extra CPUs to reserve in case the trial needs to
            launch additional Ray actors that use CPUs.
        extra_gpu: Extra GPUs to reserve in case the trial needs to
            launch additional Ray actors that use GPUs.
        extra_memory: Memory to reserve for the trial launching
            additional Ray actors that use memory.
        extra_object_store_memory: Object store memory to reserve for
            the trial launching additional Ray actors that use object store
            memory.
        custom_resources: Mapping of resource to quantity to allocate
            to the trial.
        extra_custom_resources: Extra custom resources to reserve in
            case the trial needs to launch additional Ray actors that use
            any of these custom resources.
        has_placement_group: Bool indicating if the trial also
            has an associated placement group.

    """

    __slots__ = ()

    def __new__(
        cls,
        cpu: float,
        gpu: float,
        memory: float = 0,
        object_store_memory: float = 0.0,
        extra_cpu: float = 0.0,
        extra_gpu: float = 0.0,
        extra_memory: float = 0.0,
        extra_object_store_memory: float = 0.0,
        custom_resources: Optional[dict] = None,
        extra_custom_resources: Optional[dict] = None,
        has_placement_group: bool = False,
    ):
        custom_resources = custom_resources or {}
        extra_custom_resources = extra_custom_resources or {}
        leftovers = set(custom_resources) ^ set(extra_custom_resources)

        for value in leftovers:
            custom_resources.setdefault(value, 0)
            extra_custom_resources.setdefault(value, 0)

        cpu = round(cpu, 2)
        gpu = round(gpu, 2)
        memory = round(memory, 2)
        object_store_memory = round(object_store_memory, 2)
        extra_cpu = round(extra_cpu, 2)
        extra_gpu = round(extra_gpu, 2)
        extra_memory = round(extra_memory, 2)
        extra_object_store_memory = round(extra_object_store_memory, 2)
        custom_resources = {
            resource: round(value, 2) for resource, value in custom_resources.items()
        }
        extra_custom_resources = {
            resource: round(value, 2)
            for resource, value in extra_custom_resources.items()
        }

        all_values = [
            cpu,
            gpu,
            memory,
            object_store_memory,
            extra_cpu,
            extra_gpu,
            extra_memory,
            extra_object_store_memory,
        ]
        all_values += list(custom_resources.values())
        all_values += list(extra_custom_resources.values())
        assert len(custom_resources) == len(extra_custom_resources)
        for entry in all_values:
            assert isinstance(entry, Number), ("Improper resource value.", entry)
        return super(_Resources, cls).__new__(
            cls,
            cpu,
            gpu,
            memory,
            object_store_memory,
            extra_cpu,
            extra_gpu,
            extra_memory,
            extra_object_store_memory,
            custom_resources,
            extra_custom_resources,
            has_placement_group,
        )

    def summary_string(self):
        summary = "{} CPUs, {} GPUs".format(
            self.cpu + self.extra_cpu, self.gpu + self.extra_gpu
        )
        if self.memory or self.extra_memory:
            summary += ", {} GiB heap".format(
                round((self.memory + self.extra_memory) / (1024**3), 2)
            )
        if self.object_store_memory or self.extra_object_store_memory:
            summary += ", {} GiB objects".format(
                round(
                    (self.object_store_memory + self.extra_object_store_memory)
                    / (1024**3),
                    2,
                )
            )
        custom_summary = ", ".join(
            [
                "{} {}".format(self.get_res_total(res), res)
                for res in self.custom_resources
                if not res.startswith(NODE_ID_PREFIX)
            ]
        )
        if custom_summary:
            summary += " ({})".format(custom_summary)
        return summary

    def cpu_total(self):
        return self.cpu + self.extra_cpu

    def gpu_total(self):
        return self.gpu + self.extra_gpu

    def memory_total(self):
        return self.memory + self.extra_memory

    def object_store_memory_total(self):
        return self.object_store_memory + self.extra_object_store_memory

    def get_res_total(self, key):
        return self.custom_resources.get(key, 0) + self.extra_custom_resources.get(
            key, 0
        )

    def get(self, key):
        return self.custom_resources.get(key, 0)

    def is_nonnegative(self):
        all_values = [self.cpu, self.gpu, self.extra_cpu, self.extra_gpu]
        all_values += list(self.custom_resources.values())
        all_values += list(self.extra_custom_resources.values())
        return all(v >= 0 for v in all_values)

    @classmethod
    def subtract(cls, original, to_remove):
        cpu = original.cpu - to_remove.cpu
        gpu = original.gpu - to_remove.gpu
        memory = original.memory - to_remove.memory
        object_store_memory = (
            original.object_store_memory - to_remove.object_store_memory
        )
        extra_cpu = original.extra_cpu - to_remove.extra_cpu
        extra_gpu = original.extra_gpu - to_remove.extra_gpu
        extra_memory = original.extra_memory - to_remove.extra_memory
        extra_object_store_memory = (
            original.extra_object_store_memory - to_remove.extra_object_store_memory
        )
        all_resources = set(original.custom_resources).union(
            set(to_remove.custom_resources)
        )
        new_custom_res = {
            k: original.custom_resources.get(k, 0)
            - to_remove.custom_resources.get(k, 0)
            for k in all_resources
        }
        extra_custom_res = {
            k: original.extra_custom_resources.get(k, 0)
            - to_remove.extra_custom_resources.get(k, 0)
            for k in all_resources
        }
        return _Resources(
            cpu,
            gpu,
            memory,
            object_store_memory,
            extra_cpu,
            extra_gpu,
            extra_memory,
            extra_object_store_memory,
            new_custom_res,
            extra_custom_res,
        )


class _ResourceUpdater:
    """Periodic Resource updater for Tune.

    Initially, all resources are set to 0. The updater will try to update resources
    when (1) init ResourceUpdater (2) call "update_avail_resources", "num_cpus"
    or "num_gpus".

    The update takes effect when (1) Ray is initialized (2) the interval between
    this and last update is larger than "refresh_period"
    """

    def __init__(self, refresh_period: Optional[float] = None):
        self._avail_resources = _Resources(cpu=0, gpu=0)

        if refresh_period is None:
            refresh_period = float(
                os.environ.get("TUNE_STATE_REFRESH_PERIOD", TUNE_STATE_REFRESH_PERIOD)
            )
        self._refresh_period = refresh_period
        self._last_resource_refresh = float("-inf")
        self.update_avail_resources()

    def update_avail_resources(self, num_retries: int = 5, force: bool = False):
        if not ray.is_initialized():
            return
        if (
            time.time() - self._last_resource_refresh < self._refresh_period
            and not force
        ):
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
        memory = resources.pop("memory", 0)
        object_store_memory = resources.pop("object_store_memory", 0)
        custom_resources = resources

        self._avail_resources = _Resources(
            int(num_cpus),
            int(num_gpus),
            memory=int(memory),
            object_store_memory=int(object_store_memory),
            custom_resources=custom_resources,
        )
        self._last_resource_refresh = time.time()

    def _get_used_avail_resources(self, total_allocated_resources: Dict[str, Any]):
        total_allocated_resources = total_allocated_resources.copy()

        used_cpu = total_allocated_resources.pop("CPU", 0)
        total_cpu = self._avail_resources.cpu
        used_gpu = total_allocated_resources.pop("GPU", 0)
        total_gpu = self._avail_resources.gpu

        custom_used_total = {
            name: (
                total_allocated_resources.get(name, 0.0),
                self._avail_resources.get_res_total(name),
            )
            for name in self._avail_resources.custom_resources
            if not name.startswith(NODE_ID_PREFIX)
            and (total_allocated_resources.get(name, 0.0) > 0 or "_group_" not in name)
        }
        return used_cpu, total_cpu, used_gpu, total_gpu, custom_used_total

    def debug_string(self, total_allocated_resources: Dict[str, Any]) -> str:
        """Returns a human readable message for printing to the console."""
        if self._last_resource_refresh > 0:
            (
                used_cpu,
                total_cpu,
                used_gpu,
                total_gpu,
                custom_used_total,
            ) = self._get_used_avail_resources(total_allocated_resources)

            if (
                used_cpu > total_cpu
                or used_gpu > total_gpu
                or any(used > total for (used, total) in custom_used_total.values())
            ):
                # If any of the used resources are higher than what we currently think
                # is available, update our state and re-fetch
                self.update_avail_resources(force=True)
                (
                    used_cpu,
                    total_cpu,
                    used_gpu,
                    total_gpu,
                    custom_used_total,
                ) = self._get_used_avail_resources(total_allocated_resources)

            status = (
                f"Logical resource usage: {used_cpu}/{total_cpu} CPUs, "
                f"{used_gpu}/{total_gpu} GPUs"
            )
            customs = ", ".join(
                f"{used}/{total} {name}"
                for name, (used, total) in custom_used_total.items()
            )

            if customs:
                status += f" ({customs})"
            return status
        else:
            return "Logical resource usage: ?"

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
