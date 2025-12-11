import math
import os
from typing import Any, Dict, List, Optional, Union

from .common import NodeIdStr
from ray.data._internal.execution.util import memory_string
from ray.util.annotations import DeveloperAPI


class ExecutionResources:
    """Specifies resources usage or resource limits for execution.

    By default this class represents resource usage. Use `for_limits` or
    set `default_to_inf` to True to create an object that represents resource limits.
    """

    def __init__(
        self,
        cpu: Optional[float] = None,
        gpu: Optional[float] = None,
        object_store_memory: Optional[float] = None,
        memory: Optional[float] = None,
    ):
        """Initializes ExecutionResources.
        Args:
            cpu: Amount of logical CPU slots.
            gpu: Amount of logical GPU slots.
            object_store_memory: Amount of object store memory.
            memory: Amount of logical memory in bytes.
        """

        self._cpu: float = cpu or 0.0
        self._gpu: float = gpu or 0.0
        self._object_store_memory: float = object_store_memory or 0.0
        self._memory: float = memory or 0.0

    @classmethod
    def from_resource_dict(
        cls,
        resource_dict: Dict[str, float],
    ):
        """Create an ExecutionResources object from a resource dict."""
        return ExecutionResources(
            cpu=resource_dict.get("CPU", None) or resource_dict.get("num_cpus", None),
            gpu=resource_dict.get("GPU", None) or resource_dict.get("num_gpus", None),
            object_store_memory=resource_dict.get("object_store_memory", None),
            memory=resource_dict.get("memory", None),
        )

    def to_resource_dict(self) -> Dict[str, float]:
        """Convert this ExecutionResources object to a resource dict."""

        # NOTE: Ray Core allocates fractional resources in up to 5th decimal
        #       digit, hence we round the values here up to it
        return {
            "CPU": self.cpu,
            "GPU": self.gpu,
            "object_store_memory": self.object_store_memory,
            "memory": self.memory,
        }

    @classmethod
    def for_limits(
        cls,
        cpu: Optional[float] = None,
        gpu: Optional[float] = None,
        object_store_memory: Optional[float] = None,
        memory: Optional[float] = None,
    ) -> "ExecutionResources":
        """Create an ExecutionResources object that represents resource limits.
        Args:
            cpu: Amount of logical CPU slots.
            gpu: Amount of logical GPU slots.
            object_store_memory: Amount of object store memory.
            memory: Amount of logical memory in bytes.
        """
        return ExecutionResources(
            cpu=safe_or(cpu, float("inf")),
            gpu=safe_or(gpu, float("inf")),
            object_store_memory=safe_or(object_store_memory, float("inf")),
            memory=safe_or(memory, float("inf")),
        )

    @property
    def cpu(self) -> float:
        return safe_round(self._cpu, 5)

    @property
    def gpu(self) -> float:
        return safe_round(self._gpu, 5)

    @property
    def object_store_memory(self) -> float:
        return safe_round(self._object_store_memory)

    @property
    def memory(self) -> float:
        return safe_round(self._memory)

    def __repr__(self):
        return (
            f"ExecutionResources(cpu={self.cpu}, gpu={self.gpu}, "
            f"object_store_memory={self.object_store_memory_str()}, "
            f"memory={self.memory_str()})"
        )

    def __eq__(self, other: "ExecutionResources") -> bool:
        return (
            self._cpu == other._cpu
            and self._gpu == other._gpu
            and self._object_store_memory == other._object_store_memory
            and self._memory == other._memory
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._cpu,
                self._gpu,
                self._object_store_memory,
                self._memory,
            )
        )

    @classmethod
    def zero(cls) -> "ExecutionResources":
        """Returns an ExecutionResources object with zero resources."""
        return ExecutionResources(0.0, 0.0, 0.0, 0.0)

    @classmethod
    def inf(cls) -> "ExecutionResources":
        """Returns an ExecutionResources object with infinite resources."""
        return ExecutionResources.for_limits()

    def is_zero(self) -> bool:
        """Returns True if all resources are zero."""
        return (
            self._cpu == 0.0
            and self._gpu == 0.0
            and self._object_store_memory == 0.0
            and self._memory == 0.0
        )

    def is_non_negative(self) -> bool:
        """Returns True if all resources are non-negative."""
        return (
            self._cpu >= 0
            and self._gpu >= 0
            and self._object_store_memory >= 0
            and self._memory >= 0
        )

    def object_store_memory_str(self) -> str:
        """Returns a human-readable string for the object store memory field."""
        if self._object_store_memory == float("inf"):
            return "inf"
        return memory_string(self._object_store_memory)

    def memory_str(self) -> str:
        """Returns a human-readable string for the memory field."""
        if self._memory == float("inf"):
            return "inf"
        return memory_string(self._memory)

    def copy(
        self,
        cpu: Optional[float] = None,
        gpu: Optional[float] = None,
        memory: Optional[float] = None,
        object_store_memory: Optional[float] = None,
    ) -> "ExecutionResources":
        """Returns a copy of this ExecutionResources object allowing to override
        specific resources as necessary"""
        return ExecutionResources(
            cpu=safe_or(cpu, self._cpu),
            gpu=safe_or(gpu, self._gpu),
            object_store_memory=safe_or(object_store_memory, self._object_store_memory),
            memory=safe_or(memory, self._memory),
        )

    def add(self, other: "ExecutionResources") -> "ExecutionResources":
        """Adds execution resources.

        Returns:
            A new ExecutionResource object with summed resources.
        """
        return ExecutionResources(
            cpu=self._cpu + other._cpu,
            gpu=self._gpu + other._gpu,
            object_store_memory=self._object_store_memory + other._object_store_memory,
            memory=self._memory + other._memory,
        )

    def subtract(self, other: "ExecutionResources") -> "ExecutionResources":
        """Subtracts execution resources.

        Returns:
            A new ExecutionResource object with subtracted resources.
        """
        return ExecutionResources(
            cpu=self._cpu - other._cpu,
            gpu=self._gpu - other._gpu,
            object_store_memory=self._object_store_memory - other._object_store_memory,
            memory=self._memory - other._memory,
        )

    def max(self, other: "ExecutionResources") -> "ExecutionResources":
        """Returns the maximum for each resource type."""
        return ExecutionResources(
            cpu=max(self._cpu, other._cpu),
            gpu=max(self._gpu, other._gpu),
            object_store_memory=max(
                self._object_store_memory, other._object_store_memory
            ),
            memory=max(self._memory, other._memory),
        )

    def min(self, other: "ExecutionResources") -> "ExecutionResources":
        """Returns the minimum for each resource type."""
        return ExecutionResources(
            cpu=min(self._cpu, other._cpu),
            gpu=min(self._gpu, other._gpu),
            object_store_memory=min(
                self._object_store_memory, other._object_store_memory
            ),
            memory=min(self._memory, other._memory),
        )

    def satisfies_limit(
        self,
        limit: "ExecutionResources",
        *,
        ignore_object_store_memory=False,
    ) -> bool:
        """Return if this resource struct meets the specified limits.

        Note that None for a field means no limit.

        Args:
            limit: The resource limits to check against.
            ignore_object_store_memory: If True, ignore the object store memory
                limit when checking if this resource struct meets the limits.
        """
        return (
            self._cpu <= limit._cpu
            and self._gpu <= limit._gpu
            and (
                ignore_object_store_memory
                or self._object_store_memory <= limit._object_store_memory
            )
            and self._memory <= limit._memory
        )

    def scale(self, f: float) -> "ExecutionResources":
        """Return copy with all set values scaled by `f`."""
        if f < 0:
            raise ValueError("Scaling factor must be non-negative.")
        if f == 0:
            # Explicitly handle the zero case, because `0 * inf` is undefined.
            return ExecutionResources.zero()

        return ExecutionResources(
            cpu=self._cpu * f,
            gpu=self._gpu * f,
            object_store_memory=self._object_store_memory * f,
            memory=self._memory * f,
        )

    def __truediv__(self, other: "ExecutionResources") -> "ExecutionResources":
        # NOTE: We add access each resource privately because we want to preserve the
        # decimal precision. The public properties will now on runtime safe_round to
        # 5 decimals.
        def safe_div(a: float, b: float):
            if b == 0.0:
                if a == 0.0:
                    return float("nan")
                if a > 0.0:
                    return float("inf")
                return float("-inf")
            if math.isinf(a) and math.isinf(b):
                return float("nan")
            return a / b

        return ExecutionResources(
            cpu=safe_div(self._cpu, other._cpu),
            gpu=safe_div(self._gpu, other._gpu),
            memory=safe_div(self._memory, other._memory),
            object_store_memory=safe_div(
                self._object_store_memory, other._object_store_memory
            ),
        )

    def __mul__(self, other: "ExecutionResources") -> "ExecutionResources":
        def safe_mul(a: float, b: float):
            if a == 0 and math.isinf(b):
                return float("nan")
            if math.isinf(a) and b == 0:
                return float("nan")
            return a * b

        return ExecutionResources(
            cpu=safe_mul(self._cpu, other._cpu),
            gpu=safe_mul(self._gpu, other._gpu),
            memory=safe_mul(self._memory, other._memory),
            object_store_memory=safe_mul(
                self._object_store_memory, other._object_store_memory
            ),
        )


@DeveloperAPI
class ExecutionOptions:
    """Common options for execution.

    Some options may not be supported on all executors (e.g., resource limits).

    Attributes:
        resource_limits: Set a soft limit on the resource usage during execution.
            Autodetected by default.
        exclude_resources: Amount of resources to exclude from Ray Data.
            Set this if you have other workloads running on the same cluster.
            Note,
            - If using Ray Data with Ray Train, training resources will be
            automatically excluded.
            - For each resource type, resource_limits and exclude_resources can
            not be both set.
        locality_with_output: Set this to prefer running tasks on the same node as the
            output node (node driving the execution). It can also be set to a list of
            node ids to spread the outputs across those nodes. Off by default.
        preserve_order: Set this to preserve the ordering between blocks processed by
            operators. Off by default.
        actor_locality_enabled: Whether to enable locality-aware task dispatch to
            actors (off by default). This parameter applies to both stateful map and
            streaming_split operations.
        verbose_progress: Whether to report progress individually per operator. By
            default, only AllToAll operators and global progress is reported. This
            option is useful for performance debugging. On by default.
    """

    def __init__(
        self,
        resource_limits: Optional[ExecutionResources] = None,
        exclude_resources: Optional[ExecutionResources] = None,
        locality_with_output: Union[bool, List[NodeIdStr]] = False,
        preserve_order: bool = False,
        actor_locality_enabled: bool = True,
        verbose_progress: Optional[bool] = None,
    ):
        if resource_limits is None:
            resource_limits = ExecutionResources.for_limits()
        self.resource_limits = resource_limits
        if exclude_resources is None:
            exclude_resources = ExecutionResources.zero()
        self.exclude_resources = exclude_resources
        self.locality_with_output = locality_with_output
        self.preserve_order = preserve_order
        self.actor_locality_enabled = actor_locality_enabled
        if verbose_progress is None:
            verbose_progress = bool(
                int(os.environ.get("RAY_DATA_VERBOSE_PROGRESS", "1"))
            )
        self.verbose_progress = verbose_progress

    def __repr__(self) -> str:
        return (
            f"ExecutionOptions(resource_limits={self.resource_limits}, "
            f"exclude_resources={self.exclude_resources}, "
            f"locality_with_output={self.locality_with_output}, "
            f"preserve_order={self.preserve_order}, "
            f"actor_locality_enabled={self.actor_locality_enabled}, "
            f"verbose_progress={self.verbose_progress})"
        )

    @property
    def resource_limits(self) -> ExecutionResources:
        return self._resource_limits

    @resource_limits.setter
    def resource_limits(self, value: ExecutionResources) -> None:
        self._resource_limits = ExecutionResources.for_limits(
            cpu=value._cpu,
            gpu=value._gpu,
            object_store_memory=value._object_store_memory,
            memory=value._memory,
        )

    def is_resource_limits_default(self):
        """Returns True if resource_limits is the default value."""
        return self._resource_limits == ExecutionResources.for_limits()

    def validate(self) -> None:
        """Validate the options."""
        for attr in ["cpu", "gpu", "object_store_memory"]:
            if (
                getattr(self.resource_limits, attr) != float("inf")
                and getattr(self.exclude_resources, attr, 0) > 0
            ):
                raise ValueError(
                    "resource_limits and exclude_resources cannot "
                    f" both be set for {attr} resource."
                )


def safe_or(value: Optional[Any], alt: Any) -> Any:
    return value if value is not None else alt


def safe_round(
    value: Optional[float], ndigits: Optional[int] = None
) -> Optional[float]:
    if value is None:
        return None
    elif math.isinf(value):
        return value
    else:
        return round(value, ndigits)
