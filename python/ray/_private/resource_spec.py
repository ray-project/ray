import logging
import sys
from collections import namedtuple
from typing import Optional

import ray
import ray._private.ray_constants as ray_constants


logger = logging.getLogger(__name__)

# Prefix for the node id resource that is automatically added to each node.
# For example, a node may have id `node:172.23.42.1`.
NODE_ID_PREFIX = "node:"
# The system resource that head node has.
HEAD_NODE_RESOURCE_NAME = NODE_ID_PREFIX + "__internal_head__"


class ResourceSpec(
    namedtuple(
        "ResourceSpec",
        [
            "num_cpus",
            "num_gpus",
            "memory",
            "object_store_memory",
            "resources",
        ],
    )
):
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
    """

    def __new__(
        cls,
        num_cpus=None,
        num_gpus=None,
        memory=None,
        object_store_memory=None,
        resources=None,
    ):
        return super(ResourceSpec, cls).__new__(
            cls,
            num_cpus,
            num_gpus,
            memory,
            object_store_memory,
            resources,
        )

    def resolved(self):
        """Returns if this ResourceSpec has default values filled out."""
        for v in self._asdict().values():
            if v is None:
                return False
        return True

    def to_resource_dict(self):
        """Returns a dict suitable to pass to raylet initialization.

        This renames num_cpus / num_gpus to "CPU" / "GPU",
        translates memory from bytes into 100MB memory units, and checks types.
        """
        assert self.resolved()

        resources = dict(
            self.resources,
            CPU=self.num_cpus,
            GPU=self.num_gpus,
            memory=int(self.memory),
            object_store_memory=int(self.object_store_memory),
        )

        resources = {
            resource_label: resource_quantity
            for resource_label, resource_quantity in resources.items()
            if resource_quantity != 0
        }

        # Check types.
        for resource_label, resource_quantity in resources.items():
            assert isinstance(resource_quantity, int) or isinstance(
                resource_quantity, float
            ), (
                f"{resource_label} ({type(resource_quantity)}): " f"{resource_quantity}"
            )
            if (
                isinstance(resource_quantity, float)
                and not resource_quantity.is_integer()
            ):
                raise ValueError(
                    "Resource quantities must all be whole numbers. "
                    "Violated by resource '{}' in {}.".format(resource_label, resources)
                )
            if resource_quantity < 0:
                raise ValueError(
                    "Resource quantities must be nonnegative. "
                    "Violated by resource '{}' in {}.".format(resource_label, resources)
                )
            if resource_quantity > ray_constants.MAX_RESOURCE_QUANTITY:
                raise ValueError(
                    "Resource quantities must be at most {}. "
                    "Violated by resource '{}' in {}.".format(
                        ray_constants.MAX_RESOURCE_QUANTITY, resource_label, resources
                    )
                )

        return resources

    def resolve(self, is_head: bool, node_ip_address: Optional[str] = None):
        """Returns a copy with values filled out with system defaults.

        Args:
            is_head: Whether this is the head node.
            node_ip_address: The IP address of the node that we are on.
                This is used to automatically create a node id resource.
        """

        resources = (self.resources or {}).copy()
        assert "CPU" not in resources, resources
        assert "GPU" not in resources, resources
        assert "memory" not in resources, resources
        assert "object_store_memory" not in resources, resources

        if node_ip_address is None:
            node_ip_address = ray.util.get_node_ip_address()

        # Automatically create a node id resource on each node. This is
        # queryable with ray._private.state.node_ids() and
        # ray._private.state.current_node_id().
        resources[NODE_ID_PREFIX + node_ip_address] = 1.0

        # Automatically create a head node resource.
        if HEAD_NODE_RESOURCE_NAME in resources:
            raise ValueError(
                f"{HEAD_NODE_RESOURCE_NAME}"
                " is a reserved resource name, use another name instead."
            )
        if is_head:
            resources[HEAD_NODE_RESOURCE_NAME] = 1.0

        num_cpus = self.num_cpus
        if num_cpus is None:
            num_cpus = ray._private.utils.get_num_cpus()

        num_gpus = 0
        for (
            accelerator_resource_name
        ) in ray._private.accelerators.get_all_accelerator_resource_names():
            accelerator_manager = (
                ray._private.accelerators.get_accelerator_manager_for_resource(
                    accelerator_resource_name
                )
            )
            num_accelerators = None
            if accelerator_resource_name == "GPU":
                num_accelerators = self.num_gpus
            else:
                num_accelerators = resources.get(accelerator_resource_name, None)
            visible_accelerator_ids = (
                accelerator_manager.get_current_process_visible_accelerator_ids()
            )
            # Check that the number of accelerators that the raylet wants doesn't
            # exceed the amount allowed by visible accelerator ids.
            if (
                num_accelerators is not None
                and visible_accelerator_ids is not None
                and num_accelerators > len(visible_accelerator_ids)
            ):
                raise ValueError(
                    f"Attempting to start raylet with {num_accelerators} "
                    f"{accelerator_resource_name}, "
                    f"but {accelerator_manager.get_visible_accelerator_ids_env_var()} "
                    f"contains {visible_accelerator_ids}."
                )
            if num_accelerators is None:
                # Try to automatically detect the number of accelerators.
                num_accelerators = (
                    accelerator_manager.get_current_node_num_accelerators()
                )
                # Don't use more accelerators than allowed by visible accelerator ids.
                if visible_accelerator_ids is not None:
                    num_accelerators = min(
                        num_accelerators, len(visible_accelerator_ids)
                    )

            if num_accelerators:
                if accelerator_resource_name == "GPU":
                    num_gpus = num_accelerators
                else:
                    resources[accelerator_resource_name] = num_accelerators

                accelerator_type = (
                    accelerator_manager.get_current_node_accelerator_type()
                )
                if accelerator_type:
                    resources[
                        f"{ray_constants.RESOURCE_CONSTRAINT_PREFIX}{accelerator_type}"
                    ] = 1

                    from ray._private.usage import usage_lib

                    usage_lib.record_hardware_usage(accelerator_type)
                additional_resources = (
                    accelerator_manager.get_current_node_additional_resources()
                )
                if additional_resources:
                    resources.update(additional_resources)
        # Choose a default object store size.
        system_memory = ray._private.utils.get_system_memory()
        avail_memory = ray._private.utils.estimate_available_memory()
        object_store_memory = self.object_store_memory
        if object_store_memory is None:
            object_store_memory = int(
                avail_memory * ray_constants.DEFAULT_OBJECT_STORE_MEMORY_PROPORTION
            )

            # Set the object_store_memory size to 2GB on Mac
            # to avoid degraded performance.
            # (https://github.com/ray-project/ray/issues/20388)
            if sys.platform == "darwin":
                object_store_memory = min(
                    object_store_memory, ray_constants.MAC_DEGRADED_PERF_MMAP_SIZE_LIMIT
                )

            object_store_memory_cap = (
                ray_constants.DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES
            )

            # Cap by shm size by default to avoid low performance, but don't
            # go lower than REQUIRE_SHM_SIZE_THRESHOLD.
            if sys.platform == "linux" or sys.platform == "linux2":
                # Multiple by 0.95 to give a bit of wiggle-room.
                # https://github.com/ray-project/ray/pull/23034/files
                shm_avail = ray._private.utils.get_shared_memory_bytes() * 0.95
                shm_cap = max(ray_constants.REQUIRE_SHM_SIZE_THRESHOLD, shm_avail)

                object_store_memory_cap = min(object_store_memory_cap, shm_cap)

            # Cap memory to avoid memory waste and perf issues on large nodes
            if (
                object_store_memory_cap
                and object_store_memory > object_store_memory_cap
            ):
                logger.debug(
                    "Warning: Capping object memory store to {}GB. ".format(
                        object_store_memory_cap // 1e9
                    )
                    + "To increase this further, specify `object_store_memory` "
                    "when calling ray.init() or ray start."
                )
                object_store_memory = object_store_memory_cap

        memory = self.memory
        if memory is None:
            memory = avail_memory - object_store_memory
            if memory < 100e6 and memory < 0.05 * system_memory:
                raise ValueError(
                    "After taking into account object store and redis memory "
                    "usage, the amount of memory on this node available for "
                    "tasks and actors ({} GB) is less than {}% of total. "
                    "You can adjust these settings with "
                    "ray.init(memory=<bytes>, "
                    "object_store_memory=<bytes>).".format(
                        round(memory / 1e9, 2), int(100 * (memory / system_memory))
                    )
                )

        spec = ResourceSpec(
            num_cpus,
            num_gpus,
            memory,
            object_store_memory,
            resources,
        )
        assert spec.resolved()
        return spec
