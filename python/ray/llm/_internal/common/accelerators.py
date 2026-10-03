"""Shared accelerator configurations and backend abstractions for LLM serving and batch inference."""

import copy
import math
from abc import ABC, abstractmethod
from collections import Counter
from enum import Enum
from functools import partial
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator
from typing_extensions import Annotated

import ray.util.accelerators.accelerators as accelerators
from ray._private.accelerators.tpu import (
    get_chips_per_host,
    get_num_chips_from_topology,
)
from ray.llm._internal.common.observability.logging import get_logger
from ray.util.annotations import PublicAPI
from ray.util.placement_group import PlacementGroup, placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray.util.tpu import (
    get_tpu_version_from_type,
    slice_placement_group,
)

logger = get_logger(__name__)

# CPU reservation for the Ray Data actor on each TPU host.
PARENT_ACTOR_CPU_RESERVE = 1
DEFAULT_USER_CPU_PER_HOST = 1
CPU_ACCELERATOR_TYPE_LITERAL = "CPU"


AcceleratorType = Enum("AcceleratorType", vars(accelerators))

# Set of TPU string values from Ray's known accelerators.
TPU_ACCELERATOR_VALUES = {
    member.value
    for name, member in AcceleratorType.__members__.items()
    if name.startswith("GOOGLE_TPU")
}


def normalize_tpu_accelerator_type(accelerator_type_str: str) -> str:
    """Normalize a TPU accelerator type string to uppercase standard form."""
    return accelerator_type_str.strip().upper().replace("_", "-")


def validate_tpu_accelerator_type(value: str) -> str:
    """Normalize and validate a TPU accelerator type; raise ValueError if unknown."""
    canonical = normalize_tpu_accelerator_type(value)
    if canonical not in TPU_ACCELERATOR_VALUES:
        raise ValueError(
            f"Unknown or unsupported TPU accelerator type: {value!r}. "
            f"Supported TPU types: {sorted(TPU_ACCELERATOR_VALUES)}."
        )
    return canonical


def format_ray_accelerator_resource(accelerator_type_str: str) -> str:
    """Formats the accelerator type into a Ray custom resource string."""
    return f"accelerator_type:{accelerator_type_str}"


def infer_hardware_kind_from_bundles(
    placement_group_config: Optional[Dict[str, Any]],
) -> Optional[str]:
    """Inspects placement group bundles and returns the inferred hardware kind."""
    if not placement_group_config:
        return None

    bundle_per_worker = placement_group_config.get("bundle_per_worker") or {}
    bundles = placement_group_config.get("bundles") or []
    all_bundles = [bundle_per_worker] + bundles

    if any(b.get("TPU", 0) > 0 for b in all_bundles):
        return "tpu"
    if any(b.get("GPU", 0) > 0 for b in all_bundles):
        return "gpu"

    # If a config was provided but lacks GPUs or TPUs, it is a CPU deployment
    return "cpu"


class AcceleratorConfig(BaseModel):
    kind: str


@PublicAPI(stability="alpha")
class CPUConfig(AcceleratorConfig):
    """CPU configuration. Exists for Serve parity; rejected by vLLM batch inference."""

    kind: Literal["cpu"] = "cpu"


@PublicAPI(stability="alpha")
class GPUConfig(AcceleratorConfig):
    kind: Literal["gpu"] = "gpu"


@PublicAPI(stability="alpha")
class TPUConfig(AcceleratorConfig):
    kind: Literal["tpu"] = "tpu"
    topology: Optional[str] = Field(
        default=None,
        description="The physical TPU topology (e.g. '4x4'). Required for multi-host slice gang-scheduling.",
    )
    chips_per_vm: Optional[int] = Field(
        default=None,
        description="Overrides the number of chips per host for ambiguous topologies (e.g. v6e '2x4').",
    )
    head_reservation_timeout_s: Optional[float] = Field(
        default=None,
        description="Timeout in seconds to wait for TPU slice placement group readiness.",
    )

    @field_validator("topology", mode="before")
    @classmethod
    def _normalize_topology(cls, value):
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError(f"topology must be a string; got {value!r}.")
        normalized = value.strip().lower()
        if not normalized:
            raise ValueError("topology must be a non-empty string.")
        return normalized

    @field_validator("chips_per_vm", mode="before")
    @classmethod
    def _reject_bool_chips_per_vm(cls, value):
        # bool is a subclass of int; reject before Pydantic coerces True to 1.
        if isinstance(value, bool):
            raise ValueError(f"chips_per_vm must be a positive integer; got {value!r}.")
        return value

    @field_validator("head_reservation_timeout_s", mode="before")
    @classmethod
    def _validate_head_reservation_timeout_s(cls, value):
        if value is None:
            return value
        if isinstance(value, bool):
            raise ValueError(
                f"head_reservation_timeout_s must be a positive float; got {value!r}."
            )
        try:
            val = float(value)
        except (ValueError, TypeError):
            raise ValueError(
                f"head_reservation_timeout_s must be a positive float; got {value!r}."
            )
        if val <= 0 or math.isinf(val) or math.isnan(val):
            raise ValueError(
                f"head_reservation_timeout_s must be a positive finite float; got {value!r}."
            )
        return val

    @model_validator(mode="after")
    def _validate_chips_per_vm(self) -> "TPUConfig":
        if self.chips_per_vm is not None and not self.topology:
            raise ValueError("chips_per_vm requires topology to be specified.")
        if self.chips_per_vm is not None and self.chips_per_vm <= 0:
            raise ValueError(
                "chips_per_vm must be a positive integer; "
                f"got {self.chips_per_vm!r}."
            )
        if self.topology is not None:
            try:
                total_chips = get_num_chips_from_topology(self.topology)
            except Exception as exc:
                raise ValueError(
                    f"Invalid TPU topology {self.topology!r}. Expected a chip "
                    f"topology such as '4x4' or '2x2x1'."
                ) from exc
            if total_chips <= 0:
                raise ValueError(
                    f"Invalid TPU topology {self.topology!r}. Expected a chip "
                    f"topology such as '4x4' or '2x2x1'."
                )
            if self.chips_per_vm is not None and total_chips % self.chips_per_vm != 0:
                raise ValueError(
                    f"chips_per_vm ({self.chips_per_vm}) must divide the topology "
                    f"chip count ({total_chips} for '{self.topology}')."
                )
        return self


AnyAcceleratorConfig = Annotated[
    Union[CPUConfig, GPUConfig, TPUConfig],
    Field(discriminator="kind"),
]


class AcceleratorBackend(ABC):
    @abstractmethod
    def default_bundles(
        self,
        *,
        num_devices: int,
        accelerator_type_str: Optional[str] = None,
    ) -> List[Dict[str, float]]:
        pass

    @abstractmethod
    def create_placement_group(
        self,
        *,
        bundles: List[Dict[str, float]],
        strategy: str,
        name: str,
        accelerator_type_str: Optional[str] = None,
    ) -> PlacementGroup:
        pass

    @property
    def requires_deferred_placement_group(self) -> bool:
        """
        If True, Ray Serve will not provision a placement group for the deployment.
        Instead, creation is deferred to the replica at runtime.
        Defaults to False.
        """
        return False

    @property
    @abstractmethod
    def requires_remote_initialization(self) -> bool:
        """Boolean indicating whether this backend needs a remote Ray task to query hardware during init."""
        pass

    @abstractmethod
    def get_remote_options(self, accelerator_type_str: str = None) -> Dict[str, Any]:
        """Returns the hardware-specific kwargs for ray.remote().options()."""
        pass

    def shutdown(self) -> None:
        """Release any resources owned by this backend. Idempotent."""
        return

    def build_batch_scheduling_options(
        self,
        *,
        accelerator_type: Optional[str],
        engine_kwargs: Dict[str, Any],
        placement_group_config: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Provide Ray Data scheduling options for batch inference.

        Implementations may populate accelerator-specific defaults in
        ``engine_kwargs``; callers should pass a private mutable copy. Backends
        without batch support raise ``NotImplementedError``.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement batch scheduling options."
        )


class CPUAccelerator(AcceleratorBackend):
    def __init__(self, config: Optional[CPUConfig] = None):
        self._config = config or CPUConfig()

    def default_bundles(
        self, *, num_devices: int, accelerator_type_str: Optional[str] = None
    ):
        return [{"CPU": 1} for _ in range(num_devices)]

    def create_placement_group(
        self,
        *,
        bundles: List[Dict[str, float]],
        strategy: str,
        name: str,
        accelerator_type_str: Optional[str] = None,
    ):
        return placement_group(bundles=bundles, strategy=strategy, name=name)

    @property
    def requires_remote_initialization(self) -> bool:
        return False

    def get_remote_options(self, accelerator_type_str: str = None):
        return {}


class GPUAccelerator(AcceleratorBackend):
    def __init__(self, config: Optional[GPUConfig] = None):
        self._config = config or GPUConfig()

    @staticmethod
    def _scheduling_strategy_fn(
        num_bundles_per_replica: int,
        accelerator_type: Optional[str] = None,
        placement_group_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create a PlacementGroupSchedulingStrategy for GPU engine workers."""
        if placement_group_config:
            placement_group_config = copy.deepcopy(placement_group_config)
            if accelerator_type:
                for bundle in placement_group_config["bundles"]:
                    bundle[f"accelerator_type:{accelerator_type}"] = 0.001
            pg = placement_group(**placement_group_config)
        else:
            bundle = {"GPU": 1, "CPU": 1}
            if accelerator_type:
                bundle[f"accelerator_type:{accelerator_type}"] = 0.001
            pg = placement_group(
                [bundle] * num_bundles_per_replica,
                strategy="PACK",
            )
        return {
            "scheduling_strategy": PlacementGroupSchedulingStrategy(
                pg, placement_group_capture_child_tasks=True
            )
        }

    def build_batch_scheduling_options(
        self,
        *,
        accelerator_type: Optional[str],
        engine_kwargs: Dict[str, Any],
        placement_group_config: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Provide Ray Data scheduling options for GPU batch inference."""
        tp_size = engine_kwargs.get("tensor_parallel_size", 1)
        pp_size = engine_kwargs.get("pipeline_parallel_size", 1)
        num_bundles_per_replica = tp_size * pp_size

        engine_kwargs.setdefault(
            "distributed_executor_backend",
            "uni" if num_bundles_per_replica == 1 else "ray",
        )
        executor_backend = engine_kwargs.get("distributed_executor_backend")

        if placement_group_config is not None:
            placement_group_config = copy.deepcopy(placement_group_config)
            bundle_per_worker = placement_group_config.pop("bundle_per_worker", None)
            if bundle_per_worker is not None:
                placement_group_config["bundles"] = [
                    bundle_per_worker.copy() for _ in range(num_bundles_per_replica)
                ]

        map_batches_kwargs = {}
        if accelerator_type:
            map_batches_kwargs["accelerator_type"] = accelerator_type

        if executor_backend == "ray":
            map_batches_kwargs["ray_remote_args_fn"] = partial(
                self._scheduling_strategy_fn,
                num_bundles_per_replica,
                accelerator_type,
                placement_group_config,
            )
            map_batches_kwargs["num_gpus"] = 0
        else:
            if not placement_group_config:
                # Default to GPUs per bundle if placement group is not specified.
                map_batches_kwargs["num_gpus"] = num_bundles_per_replica
            else:
                bundles = placement_group_config["bundles"]
                resource_counter = Counter()
                for bundle in bundles:
                    resource_counter.update(bundle)

                total_cpus = resource_counter.pop("CPU", 0)
                total_gpus = resource_counter.pop("GPU", 0)
                if total_cpus:
                    map_batches_kwargs["num_cpus"] = total_cpus
                if total_gpus:
                    map_batches_kwargs["num_gpus"] = total_gpus
                if resource_counter:
                    # Ray Data expects CPU/GPU via num_cpus/num_gpus, not inside `resources`.
                    map_batches_kwargs["resources"] = dict(resource_counter)

        return map_batches_kwargs

    def default_bundles(
        self, *, num_devices: int, accelerator_type_str: Optional[str] = None
    ):
        bundle = {"GPU": 1}
        if accelerator_type_str:
            bundle[format_ray_accelerator_resource(accelerator_type_str)] = 0.001
        return [bundle.copy() for _ in range(num_devices)]

    def create_placement_group(
        self,
        *,
        bundles: List[Dict[str, float]],
        strategy: str,
        name: str,
        accelerator_type_str: Optional[str] = None,
    ):
        return placement_group(bundles=bundles, strategy=strategy, name=name)

    @property
    def requires_remote_initialization(self) -> bool:
        return True

    def get_remote_options(self, accelerator_type_str: str = None):
        options = {"num_gpus": 0.001}
        if accelerator_type_str:
            options["accelerator_type"] = accelerator_type_str
        return options


class TPUAccelerator(AcceleratorBackend):
    """TPU backend shared by Ray Serve and Ray Data batch inference."""

    def __init__(self, config: Optional[TPUConfig] = None):
        self._config = config or TPUConfig()
        # _slice_pg_wrapper is used exclusively by Serve for stateful replica lifecycle management.
        self._slice_pg_wrapper = None

    @staticmethod
    def _scheduling_strategy_fn(
        topology: str,
        accelerator_version: str,
        resources_per_bundle: Dict[str, float],
        strategy: str,
        name: str = "",
        chips_per_vm: Optional[int] = None,
        head_reservation_timeout_s: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Create a PlacementGroupSchedulingStrategy using a TPU slice placement group.

        The SlicePlacementGroup (including its worker PG and head reservation PG) is created
        non-detached and fate-shares with the driver process.
        """
        slice_kwargs: Dict[str, Any] = {
            "topology": topology,
            "accelerator_version": accelerator_version,
            "resources_per_bundle": resources_per_bundle,
            "strategy": strategy or "PACK",
            "name": name,
        }
        if chips_per_vm is not None:
            slice_kwargs["chips_per_vm"] = chips_per_vm
        if head_reservation_timeout_s is not None:
            slice_kwargs["head_reservation_timeout_s"] = head_reservation_timeout_s
        slice_pg = slice_placement_group(**slice_kwargs)
        return {
            "scheduling_strategy": PlacementGroupSchedulingStrategy(
                placement_group=slice_pg.placement_group,
                placement_group_bundle_index=0,
                placement_group_capture_child_tasks=True,
            )
        }

    def default_bundles(
        self, *, num_devices: int, accelerator_type_str: Optional[str] = None
    ):
        if not self._config.topology:
            # Fallback to per-chip bundles if no topology is specified
            bundle = {"TPU": 1}
            if accelerator_type_str:
                bundle[format_ray_accelerator_resource(accelerator_type_str)] = 0.001
            return [bundle.copy() for _ in range(num_devices)]

        # Topology is specified, compute per-host bundles
        if not accelerator_type_str:
            raise ValueError(
                "`accelerator_type` must be specified when `topology` is present "
                "in order to compute TPU resource requirements."
            )
        topology = self._config.topology.strip().lower()
        version = get_tpu_version_from_type(accelerator_type_str)
        chips_per_host = (
            self._config.chips_per_vm
            if self._config.chips_per_vm is not None
            else get_chips_per_host(topology, version)
        )
        if chips_per_host <= 0:
            raise ValueError(
                f"Resolved chips per host must be positive, got {chips_per_host}"
            )

        # Serve passes TP*PP as num_devices and treats them as the chip count
        # for host packing.
        if num_devices > chips_per_host and num_devices % chips_per_host != 0:
            raise ValueError(
                f"num_devices ({num_devices}) must be a multiple of "
                f"chips_per_host ({chips_per_host}) for TPU topologies."
            )

        num_hosts = max(1, num_devices // chips_per_host)
        tpu_resources = min(num_devices, chips_per_host)
        bundle = {"TPU": tpu_resources}
        bundle[format_ray_accelerator_resource(accelerator_type_str)] = 0.001
        return [bundle.copy() for _ in range(num_hosts)]

    def create_placement_group(
        self,
        *,
        bundles: List[Dict[str, float]],
        strategy: str,
        name: str,
        accelerator_type_str: Optional[str] = None,
    ) -> PlacementGroup:
        if not self._config.topology:
            return placement_group(bundles=bundles, strategy=strategy, name=name)

        if not accelerator_type_str:
            raise ValueError(
                "accelerator_type must be provided for TPU slice provisioning."
            )

        # Filter for bundles that actually specify TPU resources
        if bundles:
            tpu_bundles = [b for b in bundles if b.get("TPU", 0) > 0]
            if not tpu_bundles:
                worker_bundle = {"TPU": 1}
            else:
                worker_bundle = tpu_bundles[0]
                if any(b != worker_bundle for b in tpu_bundles):
                    raise ValueError(
                        "Heterogeneous TPU bundles are not supported when `topology` is set. "
                        "A multi-host TPU slice requires homogeneous resource bundles across all workers. "
                        "Please use `bundle_per_worker` in `placement_group_config` to define uniform worker resources."
                    )
        else:
            # Default to 1 TPU per bundle.
            worker_bundle = {"TPU": 1}

        if self._slice_pg_wrapper is not None:
            logger.debug(
                "Existing TPU slice PG found. Shutting it down before creating a new one."
            )
            self.shutdown()

        version = get_tpu_version_from_type(accelerator_type_str)
        slice_kwargs: Dict[str, Any] = {
            "topology": self._config.topology.strip().lower(),
            "accelerator_version": version,
            "resources_per_bundle": worker_bundle,
            "strategy": strategy or "PACK",
            "name": name,
        }
        if self._config.chips_per_vm is not None:
            slice_kwargs["chips_per_vm"] = self._config.chips_per_vm
        if self._config.head_reservation_timeout_s is not None:
            slice_kwargs[
                "head_reservation_timeout_s"
            ] = self._config.head_reservation_timeout_s
        self._slice_pg_wrapper = slice_placement_group(**slice_kwargs)
        return self._slice_pg_wrapper.placement_group

    @property
    def requires_deferred_placement_group(self) -> bool:
        """
        If a TPU topology is specified, we defer PG creation so the replica can
        provision a `SlicePlacementGroup` at runtime. This ensures multi-host
        TPU slices are gang-scheduled atomically according to their physical
        topology rather than fragmented across the cluster.
        """
        return bool(self._config.topology)

    @property
    def requires_remote_initialization(self) -> bool:
        return True

    def get_remote_options(self, accelerator_type_str: str = None):
        # The PlacementGroupSchedulingStrategy natively handles routing the task to
        # the correct hardware. We omit TPU resource requests to avoid consuming
        # chips that the model engine workers must use.
        options: Dict[str, Any] = {"resources": {}}
        if accelerator_type_str:
            # Pin the task to the TPU accelerator to avoid scheduling on a CPU bundle.
            options["label_selector"] = {
                "ray.io/accelerator-type": accelerator_type_str
            }
        return options

    def shutdown(self) -> None:
        if self._slice_pg_wrapper is not None:
            try:
                logger.info("Shutting down TPU slice PG for server replica.")
                self._slice_pg_wrapper.shutdown()
            except Exception as e:
                logger.warning(f"Failed to shut down TPU slice PG: {e}")
            finally:
                self._slice_pg_wrapper = None

    def _resolve_batch_worker_bundle(
        self,
        placement_group_config: Optional[Dict[str, Any]],
    ) -> Dict[str, float]:
        """Resolve the per-worker resource template for one TPU bundle.

        Default omits TPU so Ray fills chips-per-VM. Explicit configs supply a
        homogeneous template. Always apply the parent-actor CPU floor so the
        Ray Data engine actor and user map work can admit onto bundle 0.
        """
        cpu_floor = float(PARENT_ACTOR_CPU_RESERVE + DEFAULT_USER_CPU_PER_HOST)
        if placement_group_config is None:
            return {"CPU": cpu_floor}

        bundle_per_worker = placement_group_config.get("bundle_per_worker")
        if bundle_per_worker is not None:
            source_bundles = [dict(bundle_per_worker)]
        elif (
            "bundles" in placement_group_config
            and placement_group_config["bundles"] is not None
        ):
            source_bundles = [
                dict(bundle) for bundle in placement_group_config["bundles"]
            ]
        else:
            raise ValueError(
                "placement_group_config must specify bundle_per_worker or bundles."
            )
        if not source_bundles:
            raise ValueError(
                "placement_group_config bundles must be non-empty when provided."
            )

        for bundle in source_bundles:
            gpu = bundle.get("GPU", 0)
            if (
                isinstance(gpu, bool)
                or not isinstance(gpu, (int, float))
                or not math.isfinite(gpu)
            ):
                raise ValueError(
                    f"GPU resources per bundle must be a finite number; got {gpu!r}."
                )
            if gpu > 0:
                raise ValueError(
                    "GPU resources are not supported in TPU batch "
                    f"placement_group_config bundles; got GPU={bundle['GPU']!r}."
                )
            if "TPU" in bundle:
                tpu = bundle["TPU"]
                if (
                    isinstance(tpu, bool)
                    or not isinstance(tpu, (int, float))
                    or not math.isfinite(tpu)
                ):
                    raise ValueError(
                        "TPU resources per bundle must be a positive integer; "
                        f"got {tpu!r}."
                    )
                if float(tpu) != int(tpu) or int(tpu) <= 0:
                    raise ValueError(
                        "TPU resources per bundle must be a positive integer; "
                        f"got {tpu!r}."
                    )

        has_positive_tpu = [bundle.get("TPU", 0) > 0 for bundle in source_bundles]
        if any(has_positive_tpu) and not all(has_positive_tpu):
            raise ValueError(
                "TPU batch placement_group_config bundles cannot mix TPU-bearing "
                "and non-TPU bundles."
            )

        if len(source_bundles) > 1:
            logger.warning(
                "placement_group_config specified %d bundles, but TPU batch "
                "scheduling derives the bundle count from topology %r. Using "
                "bundles[0] as a homogeneous per-worker template; the extra %d "
                "entries only participate in the homogeneity check.",
                len(source_bundles),
                self._config.topology,
                len(source_bundles) - 1,
            )

        if any(has_positive_tpu):
            worker_bundle = dict(source_bundles[0])
            if any(b != source_bundles[0] for b in source_bundles):
                raise ValueError(
                    "Heterogeneous TPU bundles are not supported when `topology` is set. "
                    "Use `bundle_per_worker` in `placement_group_config` for a uniform "
                    "per-worker resource template."
                )
        else:
            # No positive TPU: keep CPU/custom resources and omit TPU so
            # SlicePlacementGroup fills chips-per-VM (same as the default path).
            cleaned = [
                {k: v for k, v in b.items() if v != 0 and v != 0.0}
                for b in source_bundles
            ]
            if any(b != cleaned[0] for b in cleaned):
                raise ValueError(
                    "Heterogeneous placement_group_config bundles are not supported "
                    f"when `topology` is set; got {source_bundles!r}."
                )
            worker_bundle = cleaned[0]

        out = {k: v for k, v in worker_bundle.items() if v != 0 and v != 0.0}
        requested_cpu = float(out.get("CPU", 0.0))
        if 0 < requested_cpu < cpu_floor:
            logger.warning(
                "Raising placement_group_config CPU from %s to %s so the Ray Data "
                "actor can admit onto bundle 0.",
                requested_cpu,
                cpu_floor,
            )
        out["CPU"] = max(requested_cpu, cpu_floor)
        return out

    def build_batch_scheduling_options(
        self,
        *,
        accelerator_type: Optional[str],
        engine_kwargs: Dict[str, Any],
        placement_group_config: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Provide Ray Data scheduling options for TPU batch inference.

        Returns ``map_batches_kwargs`` containing a ``ray_remote_args_fn`` that
        lazily triggers TPU slice placement when invoked by Ray Data at actor
        creation time. The slice reservation is bounded by
        ``head_reservation_timeout_s`` on multi-host topologies.
        """
        if not self._config.topology:
            raise ValueError(
                "TPU batch inference requires accelerator_config.topology. "
                "Omit accelerator_config (or use GPUConfig) for GPU scheduling."
            )

        if not accelerator_type:
            raise ValueError("`accelerator_type` is required for TPU batch inference.")
        canonical_accel = validate_tpu_accelerator_type(accelerator_type)
        version = get_tpu_version_from_type(canonical_accel)

        engine_kwargs.setdefault("distributed_executor_backend", "ray")
        if engine_kwargs["distributed_executor_backend"] != "ray":
            raise ValueError(
                "TPU batch inference requires distributed_executor_backend='ray'; "
                f"got {engine_kwargs['distributed_executor_backend']!r}."
            )

        # Unlike GPU (which sizes a PG to tp*pp), the TPU slice size is fixed by
        # topology, so equality must count every device dimension including DP.
        tp = engine_kwargs.get("tensor_parallel_size", 1)
        pp = engine_kwargs.get("pipeline_parallel_size", 1)
        dp = engine_kwargs.get("data_parallel_size", 1)
        for name, value in (
            ("tensor_parallel_size", tp),
            ("pipeline_parallel_size", pp),
            ("data_parallel_size", dp),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer; got {value!r}.")
        num_devices = tp * pp * dp

        topology = self._config.topology.strip().lower()
        total_chips = get_num_chips_from_topology(topology)
        if num_devices != total_chips:
            raise ValueError(
                f"tensor_parallel_size * pipeline_parallel_size * "
                f"data_parallel_size must be {total_chips} for topology "
                f"'{topology}' on {version} ({total_chips} physical chips / "
                f"vLLM devices); got tensor_parallel_size={tp}, "
                f"pipeline_parallel_size={pp}, data_parallel_size={dp} "
                f"(product={num_devices})."
            )

        resources_per_bundle = self._resolve_batch_worker_bundle(placement_group_config)
        resources_per_bundle[format_ray_accelerator_resource(canonical_accel)] = 0.001

        strategy = (placement_group_config or {}).get("strategy") or "PACK"

        map_batches_kwargs = {
            # Bundle 0 CPU covers the Ray Data actor.
            "num_cpus": PARENT_ACTOR_CPU_RESERVE + DEFAULT_USER_CPU_PER_HOST,
            "num_gpus": 0,
            "resources": {},
            "ray_remote_args_fn": partial(
                self._scheduling_strategy_fn,
                topology,
                version,
                resources_per_bundle,
                strategy,
                "",
                self._config.chips_per_vm,
                self._config.head_reservation_timeout_s,
            ),
        }
        return map_batches_kwargs


def get_accelerator_backend(
    accelerator_config: AcceleratorConfig,
) -> AcceleratorBackend:
    """Return the backend implementation for a resolved accelerator config."""
    if isinstance(accelerator_config, TPUConfig):
        return TPUAccelerator(accelerator_config)
    if isinstance(accelerator_config, GPUConfig):
        return GPUAccelerator(accelerator_config)
    if isinstance(accelerator_config, CPUConfig):
        return CPUAccelerator(accelerator_config)
    raise TypeError(f"Unsupported accelerator config: {accelerator_config!r}")
