import json
import logging
import os
import sys
from typing import Dict, Optional, Tuple

import ray
import ray._private.ray_constants as ray_constants
from ray._common.constants import HEAD_NODE_RESOURCE_NAME, NODE_ID_PREFIX
from ray._common.utils import RESOURCE_CONSTRAINT_PREFIX
from ray._private import accelerators
from ray._private.accelerators import AcceleratorManager

logger = logging.getLogger(__name__)


class ResourceAndLabelSpec:
    """Represents the resource and label configuration passed to a raylet.

    All fields can be None. Before starting services, resolve() should be
    called to return a ResourceAndLabelSpec with unknown values filled in with
    merged values based on the local machine and user specifications.
    """

    def __init__(
        self,
        num_cpus: Optional[int] = None,
        num_gpus: Optional[int] = None,
        memory: Optional[float] = None,
        object_store_memory: Optional[float] = None,
        resources: Optional[Dict[str, float]] = None,
        labels: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize a ResourceAndLabelSpec

        Args:
            num_cpus: The CPUs allocated for this raylet.
            num_gpus: The GPUs allocated for this raylet.
            memory: The memory allocated for this raylet.
            object_store_memory: The object store memory allocated for this raylet.
            resources: The custom resources allocated for this raylet.
            labels: The labels associated with this node. Labels can be used along
                with resources for scheduling.
        """
        self.num_cpus = num_cpus
        self.num_gpus = num_gpus
        self.memory = memory
        self.object_store_memory = object_store_memory
        self.resources = resources
        self.labels = labels
        self._is_resolved = False

    def resolved(self) -> bool:
        """Returns if resolve() has been called for this ResourceAndLabelSpec
        and default values are filled out."""
        return self._is_resolved

    def _all_fields_set(self) -> bool:
        """Returns whether all fields in this ResourceAndLabelSpec are not None."""
        return all(
            v is not None
            for v in (
                self.num_cpus,
                self.num_gpus,
                self.memory,
                self.object_store_memory,
                self.resources,
                self.labels,
            )
        )

    def to_resource_dict(self):
        """Returns a dict suitable to pass to raylet initialization.

        This renames num_cpus / num_gpus to "CPU" / "GPU",
        and check types and values.
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

    def resolve(
        self, is_head: bool, node_ip_address: Optional[str] = None
    ) -> "ResourceAndLabelSpec":
        """Fills out this ResourceAndLabelSpec instance with merged values from system defaults and user specification.

        Args:
            is_head: Whether this is the head node.
            node_ip_address: The IP address of the node that we are on.
                This is used to automatically create a node id resource.

        Returns:
            ResourceAndLabelSpec: This instance with all fields resolved.
        """

        self._resolve_resources(is_head=is_head, node_ip_address=node_ip_address)

        # Resolve accelerator-specific resources
        (
            accelerator_manager,
            num_accelerators,
        ) = ResourceAndLabelSpec._get_current_node_accelerator(
            self.num_gpus, self.resources
        )
        self._resolve_accelerator_resources(accelerator_manager, num_accelerators)

        # Default num_gpus value if unset by user and unable to auto-detect.
        if self.num_gpus is None:
            self.num_gpus = 0

        # Resolve and merge node labels from all sources (params, env, and default).
        self._resolve_labels(accelerator_manager)

        # Resolve memory resources
        self._resolve_memory_resources()

        self._is_resolved = True
        assert self._all_fields_set()
        return self

    @staticmethod
    def _load_env_resources() -> Dict[str, float]:
        """Load resource overrides from the environment, if present."""
        env_resources = {}
        env_string = os.getenv(ray_constants.RESOURCES_ENVIRONMENT_VARIABLE)
        if env_string:
            try:
                env_resources = json.loads(env_string)
            except Exception:
                logger.exception(f"Failed to load {env_string}")
                raise
            logger.debug(f"Autoscaler overriding resources: {env_resources}.")
        return env_resources

    @staticmethod
    def _merge_resources(env_dict: Dict[str, float], params_dict: Dict[str, float]):
        """Merge environment and Ray param-provided resources, with env values taking precedence.
        Returns separated special case params (CPU/GPU/memory) and the merged resource dict.
        """
        num_cpus = env_dict.pop("CPU", None)
        num_gpus = env_dict.pop("GPU", None)
        memory = env_dict.pop("memory", None)
        object_store_memory = env_dict.pop("object_store_memory", None)

        result = params_dict.copy()
        result.update(env_dict)

        for key in set(env_dict.keys()).intersection(params_dict or {}):
            if params_dict[key] != env_dict[key]:
                logger.warning(
                    f"Autoscaler is overriding your resource: {key}: "
                    f"{params_dict[key]} with {env_dict[key]}."
                )

        return num_cpus, num_gpus, memory, object_store_memory, result

    def _resolve_resources(
        self, is_head: bool, node_ip_address: Optional[str] = None
    ) -> None:
        """Resolve CPU, GPU, and custom resources. Merges resources from environment,
        Ray params, and defaults in that order of precedence."""

        # Load environment override resources and merge with resources passed
        # in from Ray Params. Separates special case params if found in env.
        env_resources = ResourceAndLabelSpec._load_env_resources()
        (
            num_cpus,
            num_gpus,
            memory,
            object_store_memory,
            merged_resources,
        ) = ResourceAndLabelSpec._merge_resources(env_resources, self.resources or {})

        self.num_cpus = self.num_cpus if num_cpus is None else num_cpus
        self.num_gpus = self.num_gpus if num_gpus is None else num_gpus
        self.memory = self.memory if memory is None else memory
        self.object_store_memory = (
            self.object_store_memory
            if object_store_memory is None
            else object_store_memory
        )
        self.resources = merged_resources

        if node_ip_address is None:
            node_ip_address = ray.util.get_node_ip_address()

        # Automatically create a node id resource on each node. This is
        # queryable with ray._private.state.node_ids() and
        # ray._private.state.current_node_id().
        self.resources[NODE_ID_PREFIX + node_ip_address] = 1.0

        # Automatically create a head node resource.
        if HEAD_NODE_RESOURCE_NAME in self.resources:
            raise ValueError(
                f"{HEAD_NODE_RESOURCE_NAME}"
                " is a reserved resource name, use another name instead."
            )
        if is_head:
            self.resources[HEAD_NODE_RESOURCE_NAME] = 1.0

        # Auto-detect CPU count if not explicitly set
        if self.num_cpus is None:
            self.num_cpus = ray._private.utils.get_num_cpus()

    @staticmethod
    def _load_env_labels() -> Dict[str, str]:
        env_override_labels = {}
        env_override_labels_string = os.getenv(
            ray_constants.LABELS_ENVIRONMENT_VARIABLE
        )
        if env_override_labels_string:
            try:
                env_override_labels = json.loads(env_override_labels_string)
            except Exception:
                logger.exception(f"Failed to load {env_override_labels_string}")
                raise
            logger.info(f"Autoscaler overriding labels: {env_override_labels}.")

        return env_override_labels

    @staticmethod
    def _get_default_labels(
        accelerator_manager: Optional[AcceleratorManager],
    ) -> Dict[str, str]:
        default_labels = {}

        # Get environment variables populated from K8s Pod Spec
        node_group = os.environ.get(ray._raylet.NODE_TYPE_NAME_ENV, "")
        market_type = os.environ.get(ray._raylet.NODE_MARKET_TYPE_ENV, "")
        availability_region = os.environ.get(ray._raylet.NODE_REGION_ENV, "")
        availability_zone = os.environ.get(ray._raylet.NODE_ZONE_ENV, "")

        # Map environment variables to default ray node labels
        if market_type:
            default_labels[ray._raylet.RAY_NODE_MARKET_TYPE_KEY] = market_type
        if node_group:
            default_labels[ray._raylet.RAY_NODE_GROUP_KEY] = node_group
        if availability_zone:
            default_labels[ray._raylet.RAY_NODE_ZONE_KEY] = availability_zone
        if availability_region:
            default_labels[ray._raylet.RAY_NODE_REGION_KEY] = availability_region

        # Get accelerator type from AcceleratorManager
        if accelerator_manager:
            accelerator_type = accelerator_manager.get_current_node_accelerator_type()
            if accelerator_type:
                default_labels[
                    ray._raylet.RAY_NODE_ACCELERATOR_TYPE_KEY
                ] = accelerator_type

            # Set TPU specific default labels to enable multi-host scheduling.
            if accelerator_manager.get_resource_name() == "TPU":
                tpu_labels = accelerator_manager.get_current_node_accelerator_labels()
                if tpu_labels:
                    default_labels.update(tpu_labels)

        return default_labels

    def _resolve_labels(
        self, accelerator_manager: Optional[AcceleratorManager]
    ) -> None:
        """Resolve and merge environment override, user-input from params, and Ray default
        labels in that order of precedence."""

        # Start with a dictionary filled out with Ray default labels
        merged = ResourceAndLabelSpec._get_default_labels(accelerator_manager)

        # Merge user-specified labels from Ray params
        for key, val in (self.labels or {}).items():
            if key in merged and merged[key] != val:
                logger.warning(
                    f"User label is overriding Ray default label: {key}: "
                    f"{key}: {merged[key]} to "
                    f"{key}: {self.labels[key]}."
                )
            merged[key] = val

        # Merge autoscaler override labels from environment
        env_labels = ResourceAndLabelSpec._load_env_labels()
        for key, val in (env_labels or {}).items():
            if key in merged and merged[key] != val:
                logger.warning(
                    "Autoscaler is overriding your label:"
                    f"{key}: {merged[key]} to "
                    f"{key}: {env_labels[key]}."
                )
            merged[key] = val

        self.labels = merged

    def _resolve_accelerator_resources(self, accelerator_manager, num_accelerators):
        """Detect and update accelerator resources on a node."""
        if not accelerator_manager:
            return

        accelerator_resource_name = accelerator_manager.get_resource_name()
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

        if accelerator_resource_name == "GPU":
            self.num_gpus = num_accelerators
        else:
            self.resources[accelerator_resource_name] = num_accelerators

        accelerator_type = accelerator_manager.get_current_node_accelerator_type()
        if accelerator_type:
            self.resources[f"{RESOURCE_CONSTRAINT_PREFIX}{accelerator_type}"] = 1
        additional_resources = (
            accelerator_manager.get_current_node_additional_resources()
        )
        if additional_resources:
            self.resources.update(additional_resources)

    def _resolve_memory_resources(self):
        # Choose a default object store size.
        system_memory = ray._common.utils.get_system_memory()
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

        # Set the resolved memory and object_store_memory
        self.object_store_memory = object_store_memory
        self.memory = memory

    @staticmethod
    def _get_current_node_accelerator(
        num_gpus: Optional[int], resources: Dict[str, float]
    ) -> Tuple[AcceleratorManager, int]:
        """
        Returns the AcceleratorManager and accelerator count for the accelerator
        associated with this node. This assumes each node has at most one accelerator type.
        If no accelerators are present, returns None.

        The resolved accelerator count uses num_gpus (for GPUs) or resources if set, and
        otherwise falls back to the count auto-detected by the AcceleratorManager. The
        resolved accelerator count is capped by the number of visible accelerators.

        Args:
            num_gpus: GPU count (if provided by user).
            resources: Resource dictionary containing custom resource keys.

        Returns:
            Tuple[Optional[AcceleratorManager], int]: A tuple containing the accelerator
            manager (or None) the final resolved accelerator count.
        """
        for resource_name in accelerators.get_all_accelerator_resource_names():
            accelerator_manager = accelerators.get_accelerator_manager_for_resource(
                resource_name
            )
            if accelerator_manager is None:
                continue
            # Respect configured value for GPUs if set
            if resource_name == "GPU":
                num_accelerators = num_gpus
            else:
                num_accelerators = resources.get(resource_name)
            if num_accelerators is None:
                num_accelerators = (
                    accelerator_manager.get_current_node_num_accelerators()
                )
                visible_accelerator_ids = (
                    accelerator_manager.get_current_process_visible_accelerator_ids()
                )
                if visible_accelerator_ids is not None:
                    num_accelerators = min(
                        num_accelerators, len(visible_accelerator_ids)
                    )

            if num_accelerators > 0:
                return accelerator_manager, num_accelerators

        return None, 0
