import logging
from typing import Optional

import ray._common.utils
import ray._private.ray_constants as ray_constants
import ray._private.utils as utils

logger = logging.getLogger(__name__)

# See https://docs.kernel.org/admin-guide/cgroup-v2.html#weights
# for information about cpu weights
_CGROUP_CPU_MAX_WEIGHT: int = 10000


class ResourceIsolationConfig:
    """Configuration for enabling resource isolation by reserving memory and cpu for ray system processes through cgroupv2.

    Validates configuration for resource isolation by enforcing types, correct combinations of values, applying default values,
    and sanity checking cpu and memory reservations. Also, converts system_reserved_cpu into cpu.weights for cgroupv2.

    Raises:
        ValueError: On invalid inputs.

    Attributes:
        enable_resource_isolation: True if cgroupv2 based isolation of ray
            system processes is enabled.
        cgroup_path: The path for the cgroup the raylet should use to enforce
            resource isolation.
        system_reserved_cpu: The amount of cores reserved for ray system
            processes. Must be >= ray_constants.MINIMUM_SYSTEM_RESERVED_CPU_CORES
            and < the total number of cores available.
        system_reserved_memory: The amount of memory in bytes reserved
            for ray system processes. Must be >= ray_constants.MINIMUM_SYSTEM_RESERVED_MEMORY_BYTES
            and system_reserved_cpu + object_store_bytes < the total memory available.

    TODO(54703): Link documentation when it's available.
    """

    def __init__(
        self,
        enable_resource_isolation: bool = False,
        cgroup_path: Optional[str] = None,
        system_reserved_cpu: Optional[float] = None,
        system_reserved_memory: Optional[int] = None,
    ):
        self._resource_isolation_enabled = enable_resource_isolation
        self.cgroup_path = cgroup_path
        self.system_reserved_memory = system_reserved_memory
        self.system_pids = ""

        # cgroupv2 cpu.weight calculated from system_reserved_cpu assumes ray uses all available cores.
        self.system_reserved_cpu_weight: int = None

        # TODO(irabbani): this is used to ensure that object_store_memory is not added twice
        # to self._system_reserved_memory. This should be refactored in the future so that ResourceIsolationConfig
        # can take object_store_memory as a constructor parameter and be constructed fully by the constructor.
        self._constructed = False

        if not enable_resource_isolation:
            if self.cgroup_path:
                raise ValueError(
                    "cgroup_path cannot be set when resource isolation is not enabled. "
                    "Set enable_resource_isolation to True if you're using ray.init or use the "
                    "--enable-resource-isolation flag if you're using the ray cli."
                )
            if system_reserved_cpu:
                raise ValueError(
                    "system_reserved_cpu cannot be set when resource isolation is not enabled. "
                    "Set enable_resource_isolation to True if you're using ray.init or use the "
                    "--enable-resource-isolation flag if you're using the ray cli."
                )

            if self.system_reserved_memory:
                raise ValueError(
                    "system_reserved_memory cannot be set when resource isolation is not enabled. "
                    "Set enable_resource_isolation to True if you're using ray.init or use the "
                    "--enable-resource-isolation flag if you're using the ray cli."
                )
            return

        self.system_reserved_cpu_weight = self._validate_and_get_system_reserved_cpu(
            system_reserved_cpu
        )

        self.system_reserved_memory = self._validate_and_get_system_reserved_memory(
            system_reserved_memory
        )

        self.cgroup_path = self._validate_and_get_cgroup_path(cgroup_path)

    def is_enabled(self) -> bool:
        return self._resource_isolation_enabled

    def add_object_store_memory(self, object_store_memory_bytes: int):
        """Adds object_store_memory to the memory reserved for system processes.

        Args:
            object_store_memory_bytes: The amount processes. Must be >= ray_constants.MINIMUM_SYSTEM_RESERVED_CPU_CORES
                and < the total number of cores available.

        Raises:
            AssertionError: If called with resource isolation not enabled or called more than once for the same instance.
            ValueError: If the input is not an integer or if the system_reserved_memory + object_store_memory is greater
                than the total memory available on the system.

        """
        assert self.is_enabled(), (
            "Cannot add object_store_memory to system_reserved_memory when "
            "enable_resource_isolation is False."
        )
        assert not self._constructed, (
            "Cannot call add_object_store_memory more than once with an instance "
            "ResourceIsolationConfig. This is a bug in the ray code. "
        )
        self.system_reserved_memory += object_store_memory_bytes
        available_system_memory = ray._common.utils.get_system_memory()
        if self.system_reserved_memory > available_system_memory:
            raise ValueError(
                f"The total requested system_reserved_memory={self.system_reserved_memory}, calculated by "
                "object_store_bytes + system_reserved_memory, is greater than the total memory "
                f"available={available_system_memory}. Pick a smaller number of bytes for object_store_bytes "
                "or system_reserved_memory."
            )
        self._constructed = True

    def add_system_pids(self, system_pids: str):
        """A comma-separated list of pids to move into the system cgroup."""
        self.system_pids = system_pids

    @staticmethod
    def _validate_and_get_cgroup_path(cgroup_path: Optional[str]) -> str:
        """Returns the ray_constants.DEFAULT_CGROUP_PATH if cgroup_path is not specified.

        Args:
            cgroup_path: The path for the cgroup the raylet should use to enforce
                resource isolation.

        Returns:
            str: The validated cgroup path.

        Raises:
            ValueError: If cgroup_path is not a string.
        """
        if not cgroup_path:
            cgroup_path = ray_constants.DEFAULT_CGROUP_PATH

        if not isinstance(cgroup_path, str):
            raise ValueError(
                f"Invalid value={cgroup_path} for cgroup_path. "
                "Use a string to represent the path for the cgroup that the raylet should use "
                "to enable resource isolation."
            )

        return cgroup_path

    @staticmethod
    def _validate_and_get_system_reserved_cpu(
        system_reserved_cpu: Optional[float],
    ) -> int:
        """If system_reserved_cpu is specified, validates it, otherwise returns the default value.

        Validation entails checking the type, ensuring that the value is in range, and converts it
        into cpu.weights for cgroupv2. See https://docs.kernel.org/admin-guide/cgroup-v2.html#weights
        for more information.

        If system_reserved_cpu is not specified, returns a default value between
        [DEFAULT_MIN_SYSTEM_RESERVED_CPU_CORES, DEFAULT_MAX_SYSTEM_RESERVED_CPU_CORES].

        # TODO(54703): The errors from this method are user-facing and thus need
        to be linked the user-facing documentation once it's available.

        Args:
            system_reserved_cpu: The amount of cores reserved for ray system
                processes. Must be >= ray_constants.MINIMUM_SYSTEM_RESERVED_CPU_CORES
                and < the total number of cores available.

        Raises:
            ValueError: If system_reserved_cpu is specified, but invalid or if the system
                does not have enough available cpus.

        """
        available_system_cpus = utils.get_num_cpus(truncate=False)

        if available_system_cpus < ray_constants.DEFAULT_MIN_SYSTEM_RESERVED_CPU_CORES:
            raise ValueError(
                f"The available number of cpu cores on this system {available_system_cpus} is less than "
                f"the minimum amount that is required for ray's system processes. "
                f"Pick a number of cpu cores greater than or equal to {ray_constants.DEFAULT_MIN_SYSTEM_RESERVED_CPU_CORES}"
            )

        if not system_reserved_cpu:
            system_reserved_cpu = float(
                min(
                    max(
                        ray_constants.DEFAULT_MIN_SYSTEM_RESERVED_CPU_CORES,
                        ray_constants.DEFAULT_SYSTEM_RESERVED_CPU_PROPORTION
                        * available_system_cpus,
                    ),
                    ray_constants.DEFAULT_MAX_SYSTEM_RESERVED_CPU_CORES,
                )
            )

        if not (
            isinstance(system_reserved_cpu, float)
            or isinstance(system_reserved_cpu, int)
        ):
            raise ValueError(
                f"Invalid value={system_reserved_cpu} for system_reserved_cpu. "
                "Use a float to represent the number of cores that need to be reserved for "
                "ray system processes to enable resource isolation."
            )

        system_reserved_cpu = float(system_reserved_cpu)

        if system_reserved_cpu < ray_constants.DEFAULT_MIN_SYSTEM_RESERVED_CPU_CORES:
            raise ValueError(
                f"The requested system_reserved_cpu={system_reserved_cpu} is less than "
                f"the minimum number of cpus that can be used for resource isolation. "
                "Pick a number of cpu cores to reserve for ray system processes "
                f"greater than or equal to {ray_constants.DEFAULT_MIN_SYSTEM_RESERVED_CPU_CORES}"
            )

        if system_reserved_cpu >= available_system_cpus:
            raise ValueError(
                f"The requested system_reserved_cpu={system_reserved_cpu} is greater than or equal to "
                f"the number of cpus available={available_system_cpus}. "
                "Pick a smaller number of cpu cores to reserve for ray system processes."
            )

        # Converting the number of cores the user defined into cpu.weights
        # This assumes that ray is allowed to use all available CPU
        # cores and distribute them between system, worker and
        # user processes
        return int(
            (system_reserved_cpu / float(available_system_cpus))
            * _CGROUP_CPU_MAX_WEIGHT
        )

    @staticmethod
    def _validate_and_get_system_reserved_memory(
        system_reserved_memory: Optional[int],
    ) -> int:
        """If system_reserved_memory is not specified, returns the default value. Otherwise,
        checks the type, makes sure that the value is in range.

        Args:
            system_reserved_memory: The amount of memory in bytes reserved
                for ray system processes. Must be >= ray_constants.MINIMUM_SYSTEM_RESERVED_MEMORY_BYTES
                and < the total memory available.

        Returns:
            int: The validated system reserved memory in bytes.

        Raises:
            ValueError: If system_reserved_memory is specified, but invalid.
        """
        available_system_memory = ray._common.utils.get_system_memory()

        if (
            available_system_memory
            < ray_constants.DEFAULT_MIN_SYSTEM_RESERVED_MEMORY_BYTES
        ):
            raise ValueError(
                f"The available memory on this system {available_system_memory} is less than "
                f"the minimum amount that is required for ray's system processes. "
                f"Pick a number of bytes greater than or equal to {ray_constants.DEFAULT_MIN_SYSTEM_RESERVED_MEMORY_BYTES}"
            )

        if not system_reserved_memory:
            system_reserved_memory = int(
                min(
                    max(
                        ray_constants.DEFAULT_MIN_SYSTEM_RESERVED_MEMORY_BYTES,
                        ray_constants.DEFAULT_SYSTEM_RESERVED_MEMORY_PROPORTION
                        * available_system_memory,
                    ),
                    ray_constants.DEFAULT_MAX_SYSTEM_RESERVED_MEMORY_BYTES,
                )
            )

        if not isinstance(system_reserved_memory, int):
            raise ValueError(
                f"Invalid value {system_reserved_memory} for system_reserved_memory. "
                "Use an integer to represent the number bytes that need to be reserved for "
                "ray system processes to enable resource isolation."
            )

        if (
            system_reserved_memory
            < ray_constants.DEFAULT_MIN_SYSTEM_RESERVED_MEMORY_BYTES
        ):
            raise ValueError(
                f"The requested system_reserved_memory {system_reserved_memory} is less than "
                f"the minimum number of bytes that can be used for resource isolation. "
                "Pick a number of bytes to reserve for ray system processes "
                f"greater than or equal to {ray_constants.DEFAULT_MIN_SYSTEM_RESERVED_MEMORY_BYTES}"
            )

        if system_reserved_memory > available_system_memory:
            raise ValueError(
                f"The total requested system_reserved_memory={system_reserved_memory} is greater than "
                f"the amount of memory available={available_system_memory}."
            )
        return system_reserved_memory
