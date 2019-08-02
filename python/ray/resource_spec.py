from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
import logging
import multiprocessing
import os

import ray
import ray.ray_constants as ray_constants

logger = logging.getLogger(__name__)


class ResourceSpec(
        namedtuple("ResourceSpec", [
            "num_cpus", "num_gpus", "memory", "object_store_memory",
            "resources", "redis_max_memory"
        ])):
    """Represents the resource configuration passed to a raylet.

    All fields can be None. Before starting services, resolve() should be
    called to return a ResourceSpec with unknown values filled in with
    defaults based on the local machine specifications.

    Attributes:
        num_cpus: The CPUs allocated for this raylet.
        num_gpus: The GPUs allocated for this raylet.
        memory: The memory allocated for this raylet.
        object_store_memory: The object store memory allocated for this raylet.
            Note that when calling to_resource_dict(), this will be scaled down
            by 30% to account for the global plasma LRU reserve.
        resources: The custom resources allocated for this raylet.
        redis_max_memory: The max amount of memory (in bytes) to allow each
            redis shard to use. Once the limit is exceeded, redis will start
            LRU eviction of entries. This only applies to the sharded redis
            tables (task, object, and profile tables). By default, this is
            capped at 10GB but can be set higher.
    """

    __slots__ = ()

    def __new__(cls,
                num_cpus=None,
                num_gpus=None,
                memory=None,
                object_store_memory=None,
                resources=None,
                redis_max_memory=None):
        return super(ResourceSpec, cls).__new__(cls, num_cpus, num_gpus,
                                                memory, object_store_memory,
                                                resources, redis_max_memory)

    def resolved(self):
        """Returns if this ResourceSpec has default values filled out."""
        for v in self._asdict().values():
            if v is None:
                return False
        return True

    def to_resource_dict(self):
        """Returns a dict suitable to pass to raylet initialization.

        This renames num_cpus / num_gpus to "CPU" / "GPU", translates memory
        from bytes into 100MB memory units, and checks types.
        """
        assert self.resolved()

        memory_units = ray_constants.to_memory_units(
            self.memory, round_up=False)
        object_store_memory_units = ray_constants.to_memory_units(
            self.object_store_memory *
            ray_constants.PLASMA_RESERVABLE_MEMORY_FRACTION,
            round_up=False)

        resources = dict(
            self.resources,
            CPU=self.num_cpus,
            GPU=self.num_gpus,
            memory=memory_units,
            object_store_memory=object_store_memory_units)

        resources = {
            resource_label: resource_quantity
            for resource_label, resource_quantity in resources.items()
            if resource_quantity != 0
        }

        # Check types.
        for _, resource_quantity in resources.items():
            assert (isinstance(resource_quantity, int)
                    or isinstance(resource_quantity, float))
            if (isinstance(resource_quantity, float)
                    and not resource_quantity.is_integer()):
                raise ValueError(
                    "Resource quantities must all be whole numbers. "
                    "Received {}.".format(resources))
            if resource_quantity < 0:
                raise ValueError("Resource quantities must be nonnegative. "
                                 "Received {}.".format(resources))
            if resource_quantity > ray_constants.MAX_RESOURCE_QUANTITY:
                raise ValueError(
                    "Resource quantities must be at most {}.".format(
                        ray_constants.MAX_RESOURCE_QUANTITY))

        return resources

    def resolve(self, is_head):
        resources = (self.resources or {}).copy()
        assert "CPU" not in resources, resources
        assert "GPU" not in resources, resources
        assert "memory" not in resources, resources
        assert "object_store_memory" not in resources, resources

        num_cpus = self.num_cpus
        if num_cpus is None:
            num_cpus = multiprocessing.cpu_count()

        num_gpus = self.num_gpus
        gpu_ids = ray.utils.get_cuda_visible_devices()
        # Check that the number of GPUs that the raylet wants doesn't
        # excede the amount allowed by CUDA_VISIBLE_DEVICES.
        if (num_gpus is not None and gpu_ids is not None
                and num_gpus > len(gpu_ids)):
            raise Exception("Attempting to start raylet with {} GPUs, "
                            "but CUDA_VISIBLE_DEVICES contains {}.".format(
                                num_gpus, gpu_ids))
        if num_gpus is None:
            # Try to automatically detect the number of GPUs.
            num_gpus = _autodetect_num_gpus()
            # Don't use more GPUs than allowed by CUDA_VISIBLE_DEVICES.
            if gpu_ids is not None:
                num_gpus = min(num_gpus, len(gpu_ids))

        # Choose a default object store size.
        system_memory = ray.utils.get_system_memory()
        avail_memory = ray.utils.estimate_available_memory()
        object_store_memory = self.object_store_memory
        if object_store_memory is None:
            object_store_memory = int(system_memory * 0.2)
            # Cap memory to avoid memory waste and perf issues on large nodes
            if (object_store_memory >
                    ray_constants.DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES):
                logger.warning(
                    "Warning: Capping object memory store to {}GB. ".format(
                        ray_constants.DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES //
                        1e9) +
                    "To increase this further, specify `object_store_memory` "
                    "when calling ray.init() or ray start.")
                object_store_memory = (
                    ray_constants.DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES)

            # Other applications may also be using a lot of memory on the same
            # node. Try to detect when this is happening and log a warning or
            # error in more severe cases.
            object_store_fraction = object_store_memory / avail_memory
            # Escape hatch, undocumented for now.
            no_check = os.environ.get("RAY_DEBUG_DISABLE_MEM_CHECKS", False)
            if object_store_fraction > 0.9 and not no_check:
                raise ValueError(
                    "The default object store size of {} GB "
                    "will use more than 90% of the available memory on this "
                    "node ({} GB). Please reduce the object store memory size "
                    "to avoid memory contention with other applications, or "
                    "shut down the applications using this memory.".format(
                        round(object_store_memory / 1e9, 2),
                        round(avail_memory / 1e9, 2)))
            elif object_store_fraction > 0.5:
                logger.warning(
                    "WARNING: The default object store size of {} GB "
                    "will use more than 50% of the available memory on this "
                    "node ({} GB). Consider setting the object store memory "
                    "manually to a smaller size to avoid memory contention "
                    "with other applications.".format(
                        round(object_store_memory / 1e9, 2),
                        round(avail_memory / 1e9, 2)))

        redis_max_memory = self.redis_max_memory
        if redis_max_memory is None:
            redis_max_memory = min(
                ray_constants.DEFAULT_REDIS_MAX_MEMORY_BYTES,
                max(
                    int(system_memory * 0.1),
                    ray_constants.REDIS_MINIMUM_MEMORY_BYTES))
        if redis_max_memory < ray_constants.REDIS_MINIMUM_MEMORY_BYTES:
            raise ValueError(
                "Attempting to cap Redis memory usage at {} bytes, "
                "but the minimum allowed is {} bytes.".format(
                    redis_max_memory,
                    ray_constants.REDIS_MINIMUM_MEMORY_BYTES))

        memory = self.memory
        if memory is None:
            memory = (avail_memory - object_store_memory - (redis_max_memory
                                                            if is_head else 0))
            if memory < 1e9 and memory < 0.1 * system_memory:
                raise ValueError(
                    "After taking into account object store and redis memory "
                    "usage, the amount of memory on this node available for "
                    "tasks and actors ({} GB) is less than {}% of total. "
                    "Please decrease object_store_memory, "
                    "redis_max_memory, or shutting down some "
                    "applications.".format(
                        round(memory / 1e9, 2),
                        int(100 * (memory / system_memory))))
            elif memory < 0.4 * system_memory:
                logger.warning(
                    "WARNING: After taking into account object store and "
                    "redis memory "
                    "usage, the amount of memory on this node available for "
                    "tasks and actors ({} GB) is less than {}% of total. "
                    "Consider decreasing object_store_memory, "
                    "redis_max_memory, or shutting down some "
                    "applications.".format(
                        round(memory / 1e9, 2),
                        int(100 * (memory / system_memory))))

            logger.info(
                "Starting Ray with {} GB memory available for workers. "
                "You can reserve memory for actors and tasks with "
                "ray.remote(memory=<bytes>).".format(
                    round(
                        ray_constants.round_to_memory_units(
                            memory, round_up=False) / 1e9, 2)))

        spec = ResourceSpec(num_cpus, num_gpus, memory, object_store_memory,
                            resources, redis_max_memory)
        assert spec.resolved()
        return spec


def _autodetect_num_gpus():
    """Attempt to detect the number of GPUs on this machine.

    TODO(rkn): This currently assumes Nvidia GPUs and Linux.

    Returns:
        The number of GPUs if any were detected, otherwise 0.
    """
    proc_gpus_path = "/proc/driver/nvidia/gpus"
    if os.path.isdir(proc_gpus_path):
        return len(os.listdir(proc_gpus_path))
    return 0
