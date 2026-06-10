import math
import os
import warnings
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from .common import NodeIdStr
from ray.data._internal.execution.util import memory_string
from ray.util.annotations import DeveloperAPI


class ExecutionResources:
    """Specifies resources usage or resource limits for execution.

    By default this class represents resource usage. Use `for_limits` or
    set `default_to_inf` to True to create an object that represents resource limits.
    """

    # ``__slots__`` keeps instances small and makes attribute access go through
    # slot descriptors instead of a per-instance ``__dict__`` -- the scheduler
    # constructs millions of these per run (every ``add``/``subtract``/``max``/
    # ``copy`` returns a new object), so this is a hot-path win. It also removes
    # the need for a custom ``__setattr__`` immutability guard: the fields are
    # set once in ``__init__`` and never reassigned (only ``_quantized``
    # transitions None -> tuple as a lazy cache), so immutability is upheld by
    # convention rather than a per-set runtime check.
    __slots__ = ("_cpu", "_gpu", "_object_store_memory", "_memory", "_quantized")

    # Cached singletons for the two most common constants. `zero()` and `inf()`
    # are called all over the scheduler hot path (e.g. `.max(zero())`), and the
    # instances are immutable in practice -- every arithmetic op returns a new
    # object and there are no setters -- so a single shared instance is safe and
    # avoids the per-call allocation.
    _ZERO_SINGLETON: Optional["ExecutionResources"] = None
    _INF_SINGLETON: Optional["ExecutionResources"] = None

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

        # Store at native precision. None means "unspecified" -- this sentinel
        # is load-bearing: `ExecutionOptions.resource_limits` reads these raw
        # fields and feeds them to `for_limits()`, which maps None (not 0) to an
        # unlimited (inf) limit. So we must NOT coalesce None -> 0 here.
        #
        # Quantization to Ray Core's fractional-resource granularity happens at
        # the `to_resource_dict()` boundary; equality/zero/non-negative checks
        # quantize lazily via `_quantized_key()` (cached per instance after the
        # first access). Rounding on every construction was a per-op hotspot.
        self._cpu: Optional[float] = cpu
        self._gpu: Optional[float] = gpu
        self._object_store_memory: Optional[float] = object_store_memory
        self._memory: Optional[float] = memory
        self._quantized: Optional[Tuple[float, float, float, float]] = None

    def _quantized_key(self) -> Tuple[float, float, float, float]:
        """Return the (cpu, gpu, object_store_memory, memory) tuple quantized
        to Ray Core's fractional-resource granularity. Lazy-cached on the
        instance after the first call.
        """
        if self._quantized is None:
            self._quantized = (
                safe_round(self.cpu, 5),
                safe_round(self.gpu, 5),
                safe_round(self.object_store_memory, 0),
                safe_round(self.memory, 0),
            )
        return self._quantized

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
        """Convert this ExecutionResources object to a resource dict.

        Values are quantized to Ray Core's fractional-resource granularity
        (5 decimal digits for cpu/gpu, integer bytes for memory) so the
        output is suitable for passing back to Ray Core via ``.options(...)``.
        """
        cpu, gpu, osm, mem = self._quantized_key()
        return {
            "CPU": cpu,
            "GPU": gpu,
            "object_store_memory": osm,
            "memory": mem,
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

    def to_limits(self) -> "ExecutionResources":
        """Return a copy of this object interpreted as resource *limits*.

        Fields left unspecified (None) become unlimited (inf) rather than 0,
        so a partially-specified value like ``ExecutionResources(cpu=4)`` caps
        only CPU and leaves the other resources unbounded.
        """
        return ExecutionResources.for_limits(
            cpu=self._cpu,
            gpu=self._gpu,
            object_store_memory=self._object_store_memory,
            memory=self._memory,
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

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ExecutionResources):
            return NotImplemented
        # Quantize on access to absorb accumulated float drift from chained
        # arithmetic (cpu/gpu: ~1e-15 per op; memory: up to ~1e-4 over 1M ops
        # on byte-magnitude floats). Matches the legacy behavior, just paid
        # lazily at comparison time rather than per construction.
        return self._quantized_key() == other._quantized_key()

    def __hash__(self) -> int:
        # Quantize so equal-under-`__eq__` instances hash equally.
        return hash(self._quantized_key())

    @classmethod
    def zero(cls) -> "ExecutionResources":
        """Returns an ExecutionResources object with zero resources.

        Returns a cached, shared singleton (safe because instances are
        immutable in practice).
        """
        if cls._ZERO_SINGLETON is None:
            cls._ZERO_SINGLETON = ExecutionResources(0.0, 0.0, 0.0, 0.0)
        return cls._ZERO_SINGLETON

    @classmethod
    def inf(cls) -> "ExecutionResources":
        """Returns an ExecutionResources object with infinite resources.

        Returns a cached, shared singleton (safe because instances are
        immutable in practice).
        """
        if cls._INF_SINGLETON is None:
            cls._INF_SINGLETON = ExecutionResources.for_limits()
        return cls._INF_SINGLETON

    @classmethod
    def combine_sum(
        cls, resources: Iterable["ExecutionResources"]
    ) -> "ExecutionResources":
        """Sum an iterable of ``ExecutionResources`` in a single pass.

        Equivalent to ``reduce(lambda a, b: a.add(b), resources, zero())`` but
        accumulates raw floats and allocates a single result object instead of
        one intermediate per element. The per-iteration usage/budget rollups
        sum across every operator, so collapsing the fold removes O(num_ops)
        allocations from the scheduling hot path.
        """
        cpu = gpu = object_store_memory = memory = 0.0
        empty = True
        for r in resources:
            empty = False
            cpu += r.cpu
            gpu += r.gpu
            object_store_memory += r.object_store_memory
            memory += r.memory
        if empty:
            # Empty folds are common (e.g. completed-ops / downstream-ineligible
            # usage rollups on most iterations) -- reuse the shared zero.
            return cls.zero()
        return ExecutionResources(cpu, gpu, object_store_memory, memory)

    def is_zero(self) -> bool:
        """Returns True if all resources are zero."""
        # Quantize so accumulated float drift doesn't flip the result.
        cpu, gpu, osm, mem = self._quantized_key()
        return cpu == 0.0 and gpu == 0.0 and osm == 0.0 and mem == 0.0

    def is_non_negative(self) -> bool:
        """Returns True if all resources are non-negative."""
        # Quantize so accumulated float drift doesn't flip the result.
        cpu, gpu, osm, mem = self._quantized_key()
        return cpu >= 0 and gpu >= 0 and osm >= 0 and mem >= 0

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
        # Quantize on access so accumulated float drift (e.g. a budget
        # produced by chained add/subtract) doesn't flip the result. This
        # keeps `satisfies_limit` consistent with `__eq__`/`is_zero`/
        # `is_non_negative`, which also compare quantized values; otherwise
        # two structs equal under `__eq__` could disagree here, causing
        # `can_submit_new_task` to spuriously reject a task whose usage
        # drifted ~1e-15 above the budget.
        cpu, gpu, osm, mem = self._quantized_key()
        lcpu, lgpu, losm, lmem = limit._quantized_key()
        return (
            cpu <= lcpu
            and gpu <= lgpu
            and (ignore_object_store_memory or osm <= losm)
            and mem <= lmem
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
        exclude_resources: Amount of resources to exclude from Ray Data.
            Set this if you have other workloads running on the same cluster.
            Note,
            - If using Ray Data with Ray Train, training resources are
            automatically reserved and you don't need to set exclude_resources
            for them.
            - For each resource type, resource_limits and exclude_resources can
            not be both set.
        preserve_order: Set this to preserve the ordering between blocks processed by
            operators. Off by default.
        actor_locality_enabled: Whether to enable locality-aware task dispatch to
            actors (off by default). This parameter applies to both stateful map and
            streaming_split operations.
        verbose_progress: Whether to report progress individually per operator. By
            default, only AllToAll operators and global progress is reported. This
            option is useful for performance debugging. On by default.
        label_selector: A mapping of label key to label value. When set, every task
            and actor launched by this Dataset (including shuffle, sort, and
            aggregator actors) carries this label selector in its remote args,
            constraining placement to nodes whose labels satisfy the selector.
            Used to scope a Dataset to a labeled subset of the cluster (e.g.
            ``{"__subcluster__": "training"}``). Operator-level ``label_selector``
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
            exclude_resources: Resources to exclude from Ray Data.
            preserve_order: Whether to preserve block processing order.
            actor_locality_enabled: Whether to enable locality-aware dispatch for
                stateful map and streaming split operations.
            verbose_progress: Whether to report progress per operator. If None,
                read from ``RAY_DATA_VERBOSE_PROGRESS``.
            label_selector: Per-Dataset label selector applied to every task and
                actor launched by Ray Data. ``None`` means no selector is added.
        """
        if resource_limits is None:
            resource_limits = ExecutionResources.for_limits()
        self.resource_limits = resource_limits
        if exclude_resources is None:
            exclude_resources = ExecutionResources.zero()
        self.exclude_resources = exclude_resources
        self.preserve_order = preserve_order
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
        # Normalize to a limits object: unspecified fields become unlimited
        # (inf) rather than 0. Callers assign a bare ``ExecutionResources``
        # here (e.g. ``ExecutionResources(cpu=2)``) and rely on this.
        self._resource_limits = value.to_limits()

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
