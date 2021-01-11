import math
from collections import namedtuple
import logging
import os
import re
import subprocess
import sys

import ray
import ray.ray_constants as ray_constants

logger = logging.getLogger(__name__)

# Prefix for the node id resource that is automatically added to each node.
# For example, a node may have id `node:172.23.42.1`.
NODE_ID_PREFIX = "node:"


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
        for resource_label, resource_quantity in resources.items():
            assert (isinstance(resource_quantity, int)
                    or isinstance(resource_quantity, float)), (
                        f"{resource_label} ({type(resource_quantity)}): "
                        f"{resource_quantity}")
            if (isinstance(resource_quantity, float)
                    and not resource_quantity.is_integer()):
                raise ValueError(
                    "Resource quantities must all be whole numbers. "
                    "Violated by resource '{}' in {}.".format(
                        resource_label, resources))
            if resource_quantity < 0:
                raise ValueError("Resource quantities must be nonnegative. "
                                 "Violated by resource '{}' in {}.".format(
                                     resource_label, resources))
            if resource_quantity > ray_constants.MAX_RESOURCE_QUANTITY:
                raise ValueError("Resource quantities must be at most {}. "
                                 "Violated by resource '{}' in {}.".format(
                                     ray_constants.MAX_RESOURCE_QUANTITY,
                                     resource_label, resources))

        return resources

    def resolve(self, is_head, node_ip_address=None):
        """Returns a copy with values filled out with system defaults.

        Args:
            is_head (bool): Whether this is the head node.
            node_ip_address (str): The IP address of the node that we are on.
                This is used to automatically create a node id resource.
        """

        resources = (self.resources or {}).copy()
        assert "CPU" not in resources, resources
        assert "GPU" not in resources, resources
        assert "memory" not in resources, resources
        assert "object_store_memory" not in resources, resources

        if node_ip_address is None:
            node_ip_address = ray._private.services.get_node_ip_address()

        # Automatically create a node id resource on each node. This is
        # queryable with ray.state.node_ids() and ray.state.current_node_id().
        resources[NODE_ID_PREFIX + node_ip_address] = 1.0

        num_cpus = self.num_cpus
        if num_cpus is None:
            num_cpus = ray.utils.get_num_cpus()

        num_gpus = self.num_gpus
        gpu_ids = ray.utils.get_cuda_visible_devices()
        # Check that the number of GPUs that the raylet wants doesn't
        # excede the amount allowed by CUDA_VISIBLE_DEVICES.
        if (num_gpus is not None and gpu_ids is not None
                and num_gpus > len(gpu_ids)):
            raise ValueError("Attempting to start raylet with {} GPUs, "
                             "but CUDA_VISIBLE_DEVICES contains {}.".format(
                                 num_gpus, gpu_ids))
        if num_gpus is None:
            # Try to automatically detect the number of GPUs.
            num_gpus = _autodetect_num_gpus()
            # Don't use more GPUs than allowed by CUDA_VISIBLE_DEVICES.
            if gpu_ids is not None:
                num_gpus = min(num_gpus, len(gpu_ids))

        try:
            info_string = _get_gpu_info_string()
            gpu_types = _constraints_from_gpu_info(info_string)
            resources.update(gpu_types)
        except Exception:
            logger.exception("Could not parse gpu information.")

        # Choose a default object store size.
        system_memory = ray.utils.get_system_memory()
        avail_memory = ray.utils.estimate_available_memory()
        object_store_memory = self.object_store_memory
        if object_store_memory is None:
            object_store_memory = int(
                avail_memory *
                ray_constants.DEFAULT_OBJECT_STORE_MEMORY_PROPORTION)
            # Cap memory to avoid memory waste and perf issues on large nodes
            if (object_store_memory >
                    ray_constants.DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES):
                logger.debug(
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
            if memory < 100e6 and memory < 0.05 * system_memory:
                raise ValueError(
                    "After taking into account object store and redis memory "
                    "usage, the amount of memory on this node available for "
                    "tasks and actors ({} GB) is less than {}% of total. "
                    "You can adjust these settings with "
                    "ray.init(memory=<bytes>, "
                    "object_store_memory=<bytes>).".format(
                        round(memory / 1e9, 2),
                        int(100 * (memory / system_memory))))

        spec = ResourceSpec(num_cpus, num_gpus, memory, object_store_memory,
                            resources, redis_max_memory)
        assert spec.resolved()
        return spec


def _autodetect_num_gpus():
    """Attempt to detect the number of GPUs on this machine.

    TODO(rkn): This currently assumes NVIDIA GPUs on Linux.
    TODO(mehrdadn): This currently does not work on macOS.
    TODO(mehrdadn): Use a better mechanism for Windows.

    Possibly useful: tensorflow.config.list_physical_devices()

    Returns:
        The number of GPUs if any were detected, otherwise 0.
    """
    result = 0
    if sys.platform.startswith("linux"):
        proc_gpus_path = "/proc/driver/nvidia/gpus"
        if os.path.isdir(proc_gpus_path):
            result = len(os.listdir(proc_gpus_path))
    elif sys.platform == "win32":
        props = "AdapterCompatibility"
        cmdargs = ["WMIC", "PATH", "Win32_VideoController", "GET", props]
        lines = subprocess.check_output(cmdargs).splitlines()[1:]
        result = len([l.rstrip() for l in lines if l.startswith(b"NVIDIA")])
    return result


def _constraints_from_gpu_info(info_str):
    """Parse the contents of a /proc/driver/nvidia/gpus/*/information to get the
gpu model type.

    Args:
        info_str (str): The contents of the file.

    Returns:
        (str) The full model name.
    """
    if info_str is None:
        return {}
    lines = info_str.split("\n")
    full_model_name = None
    for line in lines:
        split = line.split(":")
        if len(split) != 2:
            continue
        k, v = split
        if k.strip() == "Model":
            full_model_name = v.strip()
            break
    pretty_name = _pretty_gpu_name(full_model_name)
    if pretty_name:
        constraint_name = (f"{ray_constants.RESOURCE_CONSTRAINT_PREFIX}"
                           f"{pretty_name}")
        return {constraint_name: 1}
    return {}


def _get_gpu_info_string():
    """Get the gpu type for this machine.

    TODO(Alex): All the caveats of _autodetect_num_gpus and we assume only one
    gpu type.

    Returns:
        (str) The gpu's model name.
    """
    if sys.platform.startswith("linux"):
        proc_gpus_path = "/proc/driver/nvidia/gpus"
        if os.path.isdir(proc_gpus_path):
            gpu_dirs = os.listdir(proc_gpus_path)
            if len(gpu_dirs) > 0:
                gpu_info_path = f"{proc_gpus_path}/{gpu_dirs[0]}/information"
                info_str = open(gpu_info_path).read()
                return info_str
    return None


# TODO(Alex): This pattern may not work for non NVIDIA Tesla GPUs (which have
# the form "Tesla V100-SXM2-16GB" or "Tesla K80").
GPU_NAME_PATTERN = re.compile("\w+\s+([A-Z0-9]+)")


def _pretty_gpu_name(name):
    if name is None:
        return None
    match = GPU_NAME_PATTERN.match(name)
    return match.group(1) if match else None
