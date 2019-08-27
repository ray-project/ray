from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import math
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
        reservable_object_store_memory = (
            self.object_store_memory *
            ray_constants.PLASMA_RESERVABLE_MEMORY_FRACTION)
        if (reservable_object_store_memory <
                ray_constants.MEMORY_RESOURCE_UNIT_BYTES):
            raise ValueError(
                "The minimum amount of object_store_memory that can be "
                "requested is {}, but you specified {}.".format(
                    int(
                        math.ceil(
                            ray_constants.MEMORY_RESOURCE_UNIT_BYTES /
                            ray_constants.PLASMA_RESERVABLE_MEMORY_FRACTION)),
                    self.object_store_memory))
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
        """Returns a copy with values filled out with system defaults."""

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
            object_store_memory = int(avail_memory * 0.3)
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

        redis_max_memory = self.redis_max_memory
        if redis_max_memory is None:
            redis_max_memory = min(
                ray_constants.DEFAULT_REDIS_MAX_MEMORY_BYTES,
                max(
                    int(avail_memory * 0.1),
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
            if memory < 500e6 and memory < 0.05 * system_memory:
                raise ValueError(
                    "After taking into account object store and redis memory "
                    "usage, the amount of memory on this node available for "
                    "tasks and actors ({} GB) is less than {}% of total. "
                    "You can adjust these settings with "
                    "ray.init(memory=<bytes>, "
                    "object_store_memory=<bytes>).".format(
                        round(memory / 1e9, 2),
                        int(100 * (memory / system_memory))))

        logger.info(
            "Starting Ray with {} GiB memory available for workers and up to "
            "{} GiB for objects. You can adjust these settings "
            "with ray.remote(memory=<bytes>, "
            "object_store_memory=<bytes>).".format(
                round(
                    ray_constants.round_to_memory_units(
                        memory, round_up=False) / (1024**3), 2),
                round(object_store_memory / (1024**3), 2)))

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
