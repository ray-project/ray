import functools
import math
import operator
import os
import warnings
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

from .common import NodeIdStr
from ray.data._internal.execution.util import memory_string
from ray.util.annotations import DeveloperAPI, RayDeprecationWarning


class ExecutionResources:
    """Specifies resources usage or resource limits for execution.

    By default this class represents resource usage. Use `for_limits` or
    set `default_to_inf` to True to create an object that represents resource limits.
    """

    # ``__slots__`` keeps instances small and makes attribute access go through
    # slot descriptors instead of a per-instance ``__dict__``. The scheduler
    # constructs many of these per iteration (every ``add``/``subtract``/
    # ``max``/``copy`` returns a new object), so this is a hot-path win.
    __slots__ = ("_cpu", "_gpu", "_object_store_memory", "_memory")

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

        # NOTE: Ray Core allocates fractional resources in up to 5th decimal
        #       digit, hence we round the values here up to it
        self._cpu: Optional[float] = safe_round(cpu, 5)
        self._gpu: Optional[float] = safe_round(gpu, 5)
        self._object_store_memory: Optional[float] = safe_round(object_store_memory, 0)
        self._memory: Optional[float] = safe_round(memory, 0)

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

        Returns:
            An ``ExecutionResources`` with the given limits (defaulting to
            infinity for any unspecified field).
        """
        return ExecutionResources(
            cpu=safe_or(cpu, float("inf")),
            gpu=safe_or(gpu, float("inf")),
            object_store_memory=safe_or(object_store_memory, float("inf")),
            memory=safe_or(memory, float("inf")),
        )

    @property
    def cpu(self) -> float:
        return self._cpu or 0.0

    @property
    def gpu(self) -> float:
        return self._gpu or 0.0

    @property
    def object_store_memory(self) -> float:
        return self._object_store_memory or 0

    @property
    def memory(self) -> float:
        return self._memory or 0

    def __repr__(self):
        return (
            f"ExecutionResources(cpu={self.cpu}, gpu={self.gpu}, "
            f"object_store_memory={self.object_store_memory_str()}, "
            f"memory={self.memory_str()})"
        )

    def __eq__(self, other: "ExecutionResources") -> bool:
        return (
            self.cpu == other.cpu
            and self.gpu == other.gpu
            and self.object_store_memory == other.object_store_memory
            and self.memory == other.memory
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.cpu,
                self.gpu,
                self.object_store_memory,
                self.memory,
            )
        )

    @classmethod
    @functools.cache
    def zero(cls) -> "ExecutionResources":
        """Returns an ExecutionResources object with zero resources.

        Returns a cached, shared singleton (``functools.cache`` keyed on ``cls``)
        -- ``zero()`` is called all over the scheduler hot path (e.g.
        ``.max(zero())``) and instances are immutable in practice (every
        arithmetic op returns a new object and there are no setters), so sharing
        one instance is safe and avoids the per-call allocation.
        """
        return ExecutionResources(0.0, 0.0, 0.0, 0.0)

    @classmethod
    @functools.cache
    def inf(cls) -> "ExecutionResources":
        """Returns an ExecutionResources object with infinite resources.

        Returns a cached, shared singleton (see :meth:`zero` for why this is
        safe).
        """
        return ExecutionResources.for_limits()

    @classmethod
    def combine(
        cls,
        resources: Iterable["ExecutionResources"],
        fn: Callable[[float, float], float],
    ) -> Optional["ExecutionResources"]:
        """Fold an iterable of ``ExecutionResources`` per dimension with ``fn``.

        ``fn(acc, value)`` combines two per-dimension floats -- e.g.
        ``operator.add`` for a sum, or ``max``/``min`` for an element-wise
        max/min. Accumulates raw floats in a single pass and allocates a single
        result object, instead of one intermediate per element as
        ``reduce(lambda a, b: a.<op>(b), resources)`` would.

        Seeds with the first element (so no per-``fn`` identity is needed) and
        returns ``None`` for an empty iterable, which may be a one-shot
        generator (so it's consumed exactly once).
        """
        iterator = iter(resources)
        first = next(iterator, None)
        if first is None:
            return None
        cpu = first.cpu
        gpu = first.gpu
        object_store_memory = first.object_store_memory
        memory = first.memory
        for r in iterator:
            cpu = fn(cpu, r.cpu)
            gpu = fn(gpu, r.gpu)
            object_store_memory = fn(object_store_memory, r.object_store_memory)
            memory = fn(memory, r.memory)
        return ExecutionResources(cpu, gpu, object_store_memory, memory)

    @classmethod
    def combine_sum(
        cls, resources: Iterable["ExecutionResources"]
    ) -> "ExecutionResources":
        """Sum an iterable of ``ExecutionResources`` in a single pass.

        Thin wrapper over :meth:`combine` with addition. Empty folds are common
        (e.g. completed-ops / downstream-ineligible usage rollups on most
        iterations), so an empty input reuses the shared ``zero()`` singleton
        instead of allocating.
        """
        result = cls.combine(resources, operator.add)
        return result if result is not None else cls.zero()

    def is_zero(self) -> bool:
        """Returns True if all resources are zero."""
        return (
            self.cpu == 0.0
            and self.gpu == 0.0
            and self.object_store_memory == 0.0
            and self.memory == 0.0
        )

    def is_non_negative(self) -> bool:
        """Returns True if all resources are non-negative."""
        return (
            self.cpu >= 0
            and self.gpu >= 0
            and self.object_store_memory >= 0
            and self.memory >= 0
        )

    def object_store_memory_str(self) -> str:
        """Returns a human-readable string for the object store memory field."""
        if self.object_store_memory == float("inf"):
            return "inf"
        return memory_string(self.object_store_memory)

    def memory_str(self) -> str:
        """Returns a human-readable string for the memory field."""
        if self.memory == float("inf"):
            return "inf"
        return memory_string(self.memory)

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
            cpu=safe_or(cpu, self.cpu),
            gpu=safe_or(gpu, self.gpu),
            object_store_memory=safe_or(object_store_memory, self.object_store_memory),
            memory=safe_or(memory, self.memory),
        )

    def add(self, other: "ExecutionResources") -> "ExecutionResources":
        """Adds execution resources.

        Args:
            other: The other ``ExecutionResources`` to add to this one.

        Returns:
            A new ExecutionResource object with summed resources.
        """
        return ExecutionResources(
            cpu=self.cpu + other.cpu,
            gpu=self.gpu + other.gpu,
            object_store_memory=self.object_store_memory + other.object_store_memory,
            memory=self.memory + other.memory,
        )

    def subtract(self, other: "ExecutionResources") -> "ExecutionResources":
        """Subtracts execution resources.

        Args:
            other: The other ``ExecutionResources`` to subtract from this one.

        Returns:
            A new ExecutionResource object with subtracted resources.
        """
        return ExecutionResources(
            cpu=self.cpu - other.cpu,
            gpu=self.gpu - other.gpu,
            object_store_memory=self.object_store_memory - other.object_store_memory,
            memory=self.memory - other.memory,
        )

    def max(self, other: "ExecutionResources") -> "ExecutionResources":
        """Returns the maximum for each resource type."""
        return ExecutionResources(
            cpu=max(self.cpu, other.cpu),
            gpu=max(self.gpu, other.gpu),
            object_store_memory=max(
                self.object_store_memory, other.object_store_memory
            ),
            memory=max(self.memory, other.memory),
        )

    def min(self, other: "ExecutionResources") -> "ExecutionResources":
        """Returns the minimum for each resource type."""
        return ExecutionResources(
            cpu=min(self.cpu, other.cpu),
            gpu=min(self.gpu, other.gpu),
            object_store_memory=min(
                self.object_store_memory, other.object_store_memory
            ),
            memory=min(self.memory, other.memory),
        )

    def satisfies_limit(
        self,
        limit: "ExecutionResources",
        *,
        ignore_object_store_memory: bool = False,
    ) -> bool:
        """Return if this resource struct meets the specified limits.

        Note that None for a field means no limit.

        Args:
            limit: The resource limits to check against.
            ignore_object_store_memory: If True, ignore the object store memory
                limit when checking if this resource struct meets the limits.

        Returns:
            ``True`` if every resource is within the corresponding limit.
        """
        return (
            self.cpu <= limit.cpu
            and self.gpu <= limit.gpu
            and (
                ignore_object_store_memory
                or self.object_store_memory <= limit.object_store_memory
            )
            and self.memory <= limit.memory
        )

    def scale(self, f: float) -> "ExecutionResources":
        """Return copy with all set values scaled by `f`."""
        if f < 0:
            raise ValueError("Scaling factor must be non-negative.")
        if f == 0:
            # Explicitly handle the zero case, because `0 * inf` is undefined.
            return ExecutionResources.zero()

        return ExecutionResources(
            cpu=self.cpu * f,
            gpu=self.gpu * f,
            object_store_memory=self.object_store_memory * f,
            memory=self.memory * f,
        )

    def floordiv(self, other: "ExecutionResources") -> "ExecutionResources":
        """Returns the floor division of resources."""

        def _div(a, b):
            if b == 0:
                return float("inf")
            if a == float("inf"):
                return float("inf")
            return math.floor(a / b)

        return ExecutionResources(
            cpu=_div(self.cpu, other.cpu),
            gpu=_div(self.gpu, other.gpu),
            object_store_memory=_div(
                self.object_store_memory, other.object_store_memory
            ),
            memory=_div(self.memory, other.memory),
        )


@DeveloperAPI
class ExecutionOptions:
    """Common options for execution.

    Some options may not be supported on all executors (e.g., resource limits).

    Attributes:
        resource_limits: Set a limit on the logical resources a Dataset can use.
            Autodetected by default.
        exclude_resources: Deprecated. Use ``label_selector`` to constrain Ray
            Data work to labeled nodes.
        preserve_order: Set this to preserve the ordering between blocks processed by
            operators. Off by default.
        actor_locality_enabled: Deprecated. Ray Data manages actor locality
            internally.
        verbose_progress: Whether to report progress individually per operator. By
            default, only AllToAll operators and global progress is reported. This
            option is useful for performance debugging. On by default.
        label_selector: A mapping of label key to label value. When set, every task
            and actor launched by this Dataset (including shuffle, sort, and
            aggregator actors) carries this label selector in its remote args,
            constraining placement to nodes whose labels satisfy the selector.
            Used to scope a Dataset to a labeled subset of the cluster (e.g.
            ``{"ray-subcluster": "training"}``). Operator-level ``label_selector``
            entries in ``ray_remote_args`` take precedence on key conflicts so
            existing node-pin selectors are preserved.
    """

    def __init__(
        self,
        resource_limits: Optional[ExecutionResources] = None,
        exclude_resources: Optional[ExecutionResources] = None,
        preserve_order: bool = False,
        actor_locality_enabled: bool = True,
        verbose_progress: Optional[bool] = None,
        label_selector: Optional[Dict[str, str]] = None,
    ):
        """Initialize execution options.

        Args:
            resource_limits: Limit on logical resources a Dataset can use.
                Defaults to auto-detected limits.
            exclude_resources: Deprecated. Use ``label_selector`` to constrain Ray
                Data work to labeled nodes.
            preserve_order: Whether to preserve block processing order.
            actor_locality_enabled: Deprecated. Ray Data manages actor locality
                internally.
            verbose_progress: Whether to report progress per operator. If None,
                read from ``RAY_DATA_VERBOSE_PROGRESS``.
            label_selector: Per-Dataset label selector applied to every task and
                actor launched by Ray Data. ``None`` means no selector is added.
        """
        if resource_limits is None:
            resource_limits = ExecutionResources.for_limits()
        self.resource_limits = resource_limits
        self._exclude_resources = ExecutionResources.zero()
        if exclude_resources is not None:
            self.exclude_resources = exclude_resources
        self.preserve_order = preserve_order
        self._actor_locality_enabled = True
        if actor_locality_enabled is not True:
            self.actor_locality_enabled = actor_locality_enabled
        if verbose_progress is None:
            verbose_progress = bool(
                int(os.environ.get("RAY_DATA_VERBOSE_PROGRESS", "1"))
            )
        self.verbose_progress = verbose_progress
        self.label_selector = label_selector

    def __repr__(self) -> str:
        return (
            f"ExecutionOptions(resource_limits={self.resource_limits}, "
            f"exclude_resources={self.exclude_resources}, "
            f"preserve_order={self.preserve_order}, "
            f"actor_locality_enabled={self.actor_locality_enabled}, "
            f"verbose_progress={self.verbose_progress}, "
            f"label_selector={self.label_selector})"
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

    @property
    def exclude_resources(self) -> ExecutionResources:
        return self._exclude_resources

    @exclude_resources.setter
    def exclude_resources(self, value: Optional[ExecutionResources]) -> None:
        if value is None:
            value = ExecutionResources.zero()

        warnings.warn(
            "`ExecutionOptions.exclude_resources` is deprecated and will be "
            "removed after January 2027. Use `ExecutionOptions.label_selector` "
            "to constrain Ray Data work to labeled nodes.",
            RayDeprecationWarning,
            stacklevel=2,
        )
        self._set_exclude_resources(value)

    def _set_exclude_resources(self, value: Optional[ExecutionResources]) -> None:
        """Set resources internally for Train v1 without warning users."""
        if value is None:
            value = ExecutionResources.zero()
        self._exclude_resources = value

    @property
    def actor_locality_enabled(self) -> bool:
        return self._actor_locality_enabled

    @actor_locality_enabled.setter
    def actor_locality_enabled(self, value: bool) -> None:
        warnings.warn(
            "`ExecutionOptions.actor_locality_enabled` is deprecated and will "
            "be removed after January 2027. Ray Data manages actor locality "
            "internally.",
            RayDeprecationWarning,
            stacklevel=2,
        )
        self._actor_locality_enabled = value

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

    @property
    def locality_with_output(self) -> bool:
        return False

    @locality_with_output.setter
    def locality_with_output(self, value: Union[bool, List[NodeIdStr]]) -> None:
        if value:
            warnings.warn(
                "`ExecutionOptions.locality_with_output` has been removed and is now "
                "a no-op. We don't recommend using it anymore, but if you still want "
                "to replicate its behavior, follow the instructions in this gist: "
                "https://gist.github.com/bveeramani/51e0383bb3680dd78fdfb92d76ea22a8.",
                DeprecationWarning,
                stacklevel=2,
            )


def safe_or(value: Optional[Any], alt: Any) -> Any:
    return value if value is not None else alt


def safe_round(
    value: Optional[float], ndigits: Optional[int] = None
) -> Optional[float]:
    if value is None:
        return None
    elif ndigits is None or math.isinf(value):
        return value
    else:
        return round(value, ndigits)
