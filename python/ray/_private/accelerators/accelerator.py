from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

# https://github.com/ray-project/ray/issues/54868
# Ray no longer overrides accelerator ids environment variables when the number
# of accelerators is zero. For example, if a user sets `num_gpus=0` in
# `ray.init()`, the environment variable `CUDA_VISIBLE_DEVICES` will not be
# set to an empty string.
#
# Set RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO=1 to restore the old behavior where
# Ray would override accelerator env vars even when zero accelerators are assigned.
#
RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO_ENV_VAR = "RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO"


class AcceleratorInventoryStatus(str, Enum):
    """The status of an accelerator inventory discovery.

    A successful *empty* inventory (``OK`` with zero devices) is intentionally
    distinct from ``UNAVAILABLE`` (driver/library absent or uninitializable) and
    ``UNSUPPORTED`` (no discovery extension for this family). Callers must not
    collapse these into "no accelerators found".
    """

    OK = "ok"
    UNSUPPORTED = "unsupported"
    UNAVAILABLE = "unavailable"
    ERROR = "error"


@dataclass(frozen=True)
class AcceleratorDevice:
    """A single accelerator device visible to the node agent.

    This is a read-only hardware inventory record. It describes the physical
    (or MIG) device the node agent can see, not free resources or scheduling
    guarantees.

    Attributes:
        vendor: The vendor name, e.g. "nvidia".
        resource_family: The Ray resource family name, e.g. "GPU".
        model: The driver-reported model/name, e.g. "Tesla V100-SXM2-16GB".
        uuid: The stable hardware UUID from the driver. Required and non-empty:
            a device whose UUID cannot be determined is surfaced as an error,
            never as a device with an empty or invented UUID.
        memory_total_bytes: Total physical memory in bytes.
        pci_bus_id: Normalized PCI bus ID with a numeric domain formatted to at
            least 4 hex digits, e.g. "0000:3b:00.0". None if unknown or
            malformed.
        numa_node: The CPU NUMA node closest to this device, read from Linux
            sysfs. None if unknown or not applicable (non-Linux).
        kind: "gpu" for a physical GPU or "mig" for a MIG instance.
        parent_uuid: For MIG instances, the UUID of the parent physical GPU.
            None for physical GPUs.
    """

    vendor: str
    resource_family: str
    model: str
    uuid: str
    memory_total_bytes: int
    pci_bus_id: Optional[str] = None
    numa_node: Optional[int] = None
    kind: str = "gpu"
    parent_uuid: Optional[str] = None


@dataclass(frozen=True)
class AcceleratorInventory:
    """The result of discovering a node's accelerator devices.

    Attributes:
        status: Whether discovery succeeded and, if not, why.
        devices: The discovered devices. Meaningful only when status is OK.
        status_message: Human-readable detail for non-OK statuses or partial
            failures.
    """

    status: AcceleratorInventoryStatus
    devices: List[AcceleratorDevice] = field(default_factory=list)
    status_message: Optional[str] = None


class AcceleratorManager(ABC):
    """This class contains all the functions needed for supporting
    an accelerator family in Ray."""

    @staticmethod
    @abstractmethod
    def get_resource_name() -> str:
        """Get the name of the resource representing this accelerator family.

        Returns:
            The resource name: e.g., the resource name for NVIDIA GPUs is "GPU"
        """

    @staticmethod
    @abstractmethod
    def get_visible_accelerator_ids_env_var() -> Optional[str]:
        """Get the env var that sets the ids of visible accelerators of this family.

        Returns:
            The env var for setting visible accelerator ids: e.g.,
                CUDA_VISIBLE_DEVICES for NVIDIA GPUs. Returns None for accelerator
                families that have no visible-devices env var (e.g. Apple Silicon
                MPS, which exposes a single device that cannot be selected or hidden).
        """

    @staticmethod
    @abstractmethod
    def get_current_node_num_accelerators() -> int:
        """Get the total number of accelerators of this family on the current node.

        Returns:
            The detected total number of accelerators of this family.
            Return 0 if the current node doesn't contain accelerators of this family.
        """

    @staticmethod
    @abstractmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        """Get the type of the accelerator of this family on the current node.

        Currently Ray only supports single accelerator type of
        an accelerator family on each node.

        The result should only be used when get_current_node_num_accelerators() > 0.

        Returns:
            The detected accelerator type of this family: e.g., H100 for NVIDIA GPU.
            Return None if it's unknown or the node doesn't have
            accelerators of this family.
        """

    @staticmethod
    @abstractmethod
    def get_current_node_additional_resources() -> Optional[Dict[str, float]]:
        """Get any additional resources required for the current node.

        In case a particular accelerator type requires considerations for
        additional resources (e.g. for TPUs, providing the TPU pod type and
        TPU name), this function can be used to provide the
        additional logical resources.

        Returns:
            A dictionary representing additional resources that may be
            necessary for a particular accelerator type.
        """

    @staticmethod
    @abstractmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        """Validate the resource request quantity of this accelerator resource.

        Args:
            quantity: The resource request quantity to be validated.

        Returns:
            (valid, error_message) tuple: the first element of the tuple
                indicates whether the given quantity is valid or not,
                the second element is the error message
                if the given quantity is invalid.
        """

    @staticmethod
    @abstractmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        """Get the ids of accelerators of this family that are visible to the current process.

        Returns:
            The list of visiable accelerator ids.
            Return None if all accelerators are visible.
        """

    @staticmethod
    @abstractmethod
    def set_current_process_visible_accelerator_ids(ids: List[str]) -> None:
        """Set the ids of accelerators of this family that are visible to the current process.

        Args:
            ids: The ids of visible accelerators of this family.
        """

    @staticmethod
    def get_ec2_instance_num_accelerators(
        instance_type: str, instances: dict
    ) -> Optional[int]:
        """Get the number of accelerators of this family on ec2 instance with given type.

        Args:
            instance_type: The ec2 instance type.
            instances: Map from ec2 instance type to instance metadata returned by
                ec2 `describe-instance-types`.

        Returns:
            The number of accelerators of this family on the ec2 instance
            with given type.
            Return None if it's unknown.
        """
        return None

    @staticmethod
    def get_ec2_instance_accelerator_type(
        instance_type: str, instances: dict
    ) -> Optional[str]:
        """Get the accelerator type of this family on ec2 instance with given type.

        Args:
            instance_type: The ec2 instance type.
            instances: Map from ec2 instance type to instance metadata returned by
                ec2 `describe-instance-types`.

        Returns:
            The accelerator type of this family on the ec2 instance with given type.
            Return None if it's unknown.
        """
        return None

    @staticmethod
    def get_current_node_accelerator_labels() -> Optional[Dict[str, str]]:
        """Get accelerator related Ray node labels of the curent node.

        Returns:
            A dictionary mapping accelerator related label keys to values.
        """
        return None

    @staticmethod
    def get_cdi_kind() -> Optional[str]:
        """Get the CDI (Container Device Interface) kind for this
        accelerator family, e.g. "nvidia.com/gpu".

        Returns:
            The CDI kind, or None if this accelerator family doesn't
            support CDI-based device injection (e.g. into Ray Sandboxes).
        """
        return None

    @staticmethod
    def generate_cdi_spec() -> Optional[Dict]:
        """Generate and return a CDI spec describing this accelerator
        family's devices on the current node. Only meaningful when
        `get_cdi_kind()` returns non-None. Returns the parsed spec directly
        rather than writing to disk. Implementations should avoid writing
        a spec file themselves too, so callers never have to reconcile a
        cached-on-disk spec with one generated in-process.

        Returns:
            The parsed CDI spec. This default implementation returns None,
            meaning this accelerator family doesn't implement CDI spec
            generation. A family that does implement it (e.g.
            NVIDIA's) instead raises if generation is attempted and fails.
            Callers only call this once they've already confirmed CDI is
            supported (via `get_cdi_kind()`), so at that point a failure
            is a real error, not "unsupported". Callers should let
            that exception propagate as a fatal error rather than
            treating it like this method's None return.
        """
        return None

    @staticmethod
    def get_current_node_accelerator_inventory() -> Optional[AcceleratorInventory]:
        """Discover the accelerators of this family visible to the node agent.

        This is a read-only hardware inventory of the devices the node agent
        can see (physical devices and, for families that expose them, MIG
        instances). It is NOT a free-resource count and makes no scheduling
        guarantee.

        The default implementation returns None, meaning this accelerator
        family has no inventory discovery implementation. A family that
        implements discovery returns an ``AcceleratorInventory`` whose
        ``status`` distinguishes a successful (possibly empty) inventory from
        an unsupported family, an unavailable driver, or an unexpected error.

        Returns:
            An ``AcceleratorInventory`` if this family supports discovery, or
            None if it does not.
        """
        return None
