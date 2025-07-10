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
    """Configuration for enabling resource isolation by reserving memory
    and cpu for ray system processes through cgroupv2.
    This class validates configuration for resource isolation by
    enforcing types, correct combinations of values, applying default values,
    and sanity checking cpu and memory reservations.
    Also, converts system_reserved_cpu into cpu.weights for cgroupv2.

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
        # cgroupv2 cpu.weight calculated from system_reserved_cpu
        # assumes ray uses all available cores.
        self.system_reserved_cpu_weight: int = None
        # TODO(irabbani): this is used to ensure
        # that object_store_memory is not added twice
        # to self._system_reserved_memory. This should
        # be refactored in the future so that ResourceIsolationConfig
        # can take object_store_memory as a constructor parameter
        # and be constructed fully by the constructor.
        self._constructed = False

        if not enable_resource_isolation:
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
            if self.cgroup_path:
                raise ValueError(
                    "cgroup_path cannot be set when resource isolation is not enabled. "
                    "Set enable_resource_isolation to True if you're using ray.init or use the "
                    "--enable-resource-isolation flag if you're using the ray cli."
                )
            return

        # resource isolation is enabled
        self.system_reserved_cpu_weight = self._validate_and_get_system_reserved_cpu(
            system_reserved_cpu
        )
        self.system_reserved_memory = self._validate_and_get_system_reserved_memory(
            system_reserved_memory
        )
        self.cgroup_path = self._validate_and_get_cgroup_path(cgroup_path)

    def is_enabled(self) -> bool:
        return self._resource_isolation_enabled

    def add_object_store_memory(self, object_store_memory: int):
        """This is only supposed to be called once. It also cannot be
        called if resouce isolation is not enabled.
        """
        assert self.is_enabled(), (
            "Cannot add object_store_memory to system_reserved_memory when "
            "enable_resource_isolation is False."
        )
        assert not self._constructed, (
            "Cannot add object_store_memory to system_reserved_memory when"
            "multiple times."
        )
        self.system_reserved_memory += object_store_memory
        available_system_memory = ray._common.utils.get_system_memory()
        if self.system_reserved_memory > available_system_memory:
            raise ValueError(
                f"The total requested system_reserved_memory={self.system_reserved_memory}, calculated by "
                " object_store_bytes + system_reserved_memory, is greater than the total memory "
                f" available={available_system_memory}. Pick a smaller number of bytes for object_store_bytes "
                "or system_reserved_memory."
            )
        self._constructed = True

    @staticmethod
    def _validate_and_get_cgroup_path(cgroup_path: Optional[str]) -> str:
        """Returns the ray_constants.DEFAULT_CGROUP_PATH if cgroup_path is not
        specified. Checks the type of cgroup_path.

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
        """If system_reserved_cpu is not specified, returns the default value. Otherwise,
        checks the type, makes sure that the value is in range, and converts it into cpu.weights
        for cgroupv2. See https://docs.kernel.org/admin-guide/cgroup-v2.html#weights for more information.

        Args:
            system_reserved_cpu: The amount of cores reserved for ray system
                processes. Must be >= ray_constants.MINIMUM_SYSTEM_RESERVED_CPU_CORES
                and < the total number of cores available.

        Raises:
            ValueError: If system_reserved_cpu is specified, but invalid.
        """
        available_system_cpus = utils.get_num_cpus()

        if not system_reserved_cpu:
            system_reserved_cpu = min(
                ray_constants.DEFAULT_SYSTEM_RESERVED_CPU_CORES,
                ray_constants.DEFAULT_SYSTEM_RESERVED_CPU_PROPORTION
                * available_system_cpus,
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

        if system_reserved_cpu < ray_constants.MINIMUM_SYSTEM_RESERVED_CPU_CORES:
            raise ValueError(
                f"The requested system_reserved_cpu={system_reserved_cpu} is less than "
                f"the minimum number of cpus that can be used for resource isolation. "
                "Pick a number of cpu cores to reserve for ray system processes "
                f"greater than or equal to {ray_constants.MINIMUM_SYSTEM_RESERVED_CPU_CORES}"
            )

        if system_reserved_cpu > available_system_cpus:
            raise ValueError(
                f"The requested system_reserved_cpu={system_reserved_cpu} is greater than "
                f"the number of cpus available={available_system_cpus}. "
                "Pick a smaller number of cpu cores to reserve for ray system processes."
            )

        # Converting the number of cores the user defined into cpu.weights
        # This assumes that ray is allowed to use all available CPU
        # cores and distribute them between system processes and
        # application processes
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

        if not system_reserved_memory:
            system_reserved_memory = int(
                min(
                    ray_constants.DEFAULT_SYSTEM_RESERVED_MEMORY_BYTES,
                    ray_constants.DEFAULT_SYSTEM_RESERVED_MEMORY_PROPORTION
                    * available_system_memory,
                )
            )

        if not isinstance(system_reserved_memory, int):
            raise ValueError(
                f"Invalid value={system_reserved_memory} for system_reserved_memory. "
                "Use an integer to represent the number bytes that need to be reserved for "
                "ray system processes to enable resource isolation."
            )

        if system_reserved_memory < ray_constants.MINIMUM_SYSTEM_RESERVED_MEMORY_BYTES:
            raise ValueError(
                f"The requested system_reserved_memory={system_reserved_memory} is less than "
                f"the minimum number of bytes that can be used for resource isolation. "
                "Pick a number of bytes to reserve for ray system processes "
                f"greater than or equal to {ray_constants.MINIMUM_SYSTEM_RESERVED_MEMORY_BYTES}"
            )

        if system_reserved_memory > available_system_memory:
            raise ValueError(
                f"The total requested system_reserved_memory={system_reserved_memory} is greater than "
                f"the amount of memory available={available_system_memory}."
            )
        return system_reserved_memory
