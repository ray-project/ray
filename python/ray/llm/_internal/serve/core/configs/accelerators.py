from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, Union

from pydantic import BaseModel, Field
from typing_extensions import Annotated

import ray
import ray.util.accelerators.accelerators as accelerators
from ray._common.utils import RESOURCE_CONSTRAINT_PREFIX
from ray._private.accelerators.tpu import TPUAcceleratorManager, get_chips_per_host
from ray.llm._internal.serve.observability.logging import get_logger
from ray.util.placement_group import (
    PlacementGroup,
    placement_group,
    validate_placement_group,
)
from ray.util.tpu import (
    get_tpu_version_from_type,
    get_tpu_worker_resources,
    slice_placement_group,
)

logger = get_logger(__name__)

AcceleratorType = Enum("AcceleratorType", vars(accelerators))

# Set of TPU string values from Ray's known accelerators.
TPU_ACCELERATOR_VALUES = {
    member.value
    for name, member in AcceleratorType.__members__.items()
    if name.startswith("GOOGLE_TPU")
}


def format_ray_accelerator_resource(accelerator_type_str: str) -> str:
    """Formats the accelerator type into a Ray custom resource string."""
    return f"accelerator_type:{accelerator_type_str}"


def _is_positive_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0


def prepare_tpu_bundles_and_label_selector(
    bundles: List[Dict[str, float]],
    accelerator_type_str: Optional[str] = None,
) -> Tuple[List[Dict[str, float]], Optional[List[Dict[str, str]]]]:
    """Strip accelerator_type:* resource constraints from bundles and build a
    matching per-bundle ray.io/accelerator-type label selector for TPU bundles.

    Note: ``TPUAccelerator.default_bundles`` retains ``accelerator_type:*``
    in the raw bundle list for callers that inspect ``placement_bundles``, while
    placement group creation routes TPU hardware selection through node labels
    (since KubeRay TPU worker groups advertise ``ray.io/accelerator-type`` labels
    rather than ``accelerator_type:*`` custom resources).
    """
    top_level_accel = (
        accelerator_type_str.strip()
        if isinstance(accelerator_type_str, str)
        else accelerator_type_str
    ) or None
    clean_bundles: List[Dict[str, float]] = []
    bundle_accels: List[Optional[str]] = []
    for b in bundles:
        clean_b: Dict[str, float] = {}
        bundle_accel = top_level_accel if _is_positive_number(b.get("TPU", 0)) else None
        for k, v in b.items():
            if k.startswith(RESOURCE_CONSTRAINT_PREFIX):
                if _is_positive_number(v) and bundle_accel is None:
                    suffix = k[len(RESOURCE_CONSTRAINT_PREFIX) :].strip()
                    if suffix:
                        bundle_accel = suffix
            else:
                clean_b[k] = v
        clean_bundles.append(clean_b)
        bundle_accels.append(bundle_accel)

    label_selector = (
        [
            {ray._raylet.RAY_NODE_ACCELERATOR_TYPE_KEY: accel} if accel else {}
            for accel in bundle_accels
        ]
        if any(bundle_accels)
        else None
    )
    return clean_bundles, label_selector


def normalize_bundle_label_selector(
    bundle_label_selector: Optional[Sequence[Dict[str, str]]],
) -> Optional[List[Dict[str, str]]]:
    """Validate and normalize a bundle_label_selector to None or a fresh list of dicts.

    Accepts None, a list, or a tuple of Dict[str, str]. Empty sequences normalize to
    None. Returns shallow copies of each selector dict to prevent shared mutation.
    """
    if bundle_label_selector is None:
        return None
    if not isinstance(bundle_label_selector, (list, tuple)):
        raise ValueError(
            "bundle_label_selector must be a list or tuple of string label selector "
            f"dicts, got {type(bundle_label_selector).__name__}."
        )
    if len(bundle_label_selector) == 0:
        return None

    normalized: List[Dict[str, str]] = []
    for idx, selector in enumerate(bundle_label_selector):
        if (
            not isinstance(selector, dict)
            or not all(isinstance(k, str) for k in selector.keys())
            or not all(isinstance(v, str) for v in selector.values())
        ):
            raise ValueError(
                f"bundle_label_selector entry at index {idx} must be a dict mapping "
                "string keys to string values."
            )
        normalized.append(dict(selector))
    return normalized


def _resolve_bundle_label_selector(
    num_bundles: int,
    user_selectors: Optional[Sequence[Dict[str, str]]] = None,
    base_selectors: Optional[List[Dict[str, str]]] = None,
) -> Optional[List[Dict[str, str]]]:
    """Normalize, broadcast, and merge user bundle_label_selector with base selectors.

    Returns None when neither base_selectors nor user_selectors are present.
    A single-element user selector broadcasts to all num_bundles. User keys take
    precedence over base_selectors keys on conflict.
    """
    if num_bundles < 0:
        raise ValueError(f"num_bundles must be non-negative, got {num_bundles}.")

    normalized_user = normalize_bundle_label_selector(user_selectors)

    if base_selectors is not None and len(base_selectors) != num_bundles:
        raise ValueError(
            f"base_selectors length ({len(base_selectors)}) must match "
            f"num_bundles ({num_bundles})."
        )

    if normalized_user is None:
        return (
            [dict(sel) for sel in base_selectors]
            if base_selectors is not None
            else None
        )

    if num_bundles == 0:
        raise ValueError(
            f"bundle_label_selector length ({len(normalized_user)}) cannot match "
            "0 bundles."
        )

    if len(normalized_user) == 1:
        expanded_user = [dict(normalized_user[0]) for _ in range(num_bundles)]
    elif len(normalized_user) == num_bundles:
        expanded_user = normalized_user
    else:
        raise ValueError(
            f"bundle_label_selector length ({len(normalized_user)}) must be 1 or "
            f"match the number of bundles ({num_bundles})."
        )

    if base_selectors is None:
        return expanded_user

    return [
        {**base_sel, **user_sel}
        for base_sel, user_sel in zip(base_selectors, expanded_user)
    ]


def infer_hardware_kind_from_bundles(
    placement_group_config: Optional[Dict[str, Any]]
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


class CPUConfig(AcceleratorConfig):
    kind: Literal["cpu"] = "cpu"


class GPUConfig(AcceleratorConfig):
    kind: Literal["gpu"] = "gpu"


class TPUConfig(AcceleratorConfig):
    kind: Literal["tpu"] = "tpu"
    topology: Optional[str] = None


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
        bundle_label_selector: Optional[Sequence[Dict[str, str]]] = None,
    ) -> PlacementGroup:
        pass

    @property
    def requires_deferred_placement_group(self) -> bool:
        """If True, Ray Serve will not provision a placement group for the deployment.
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


class CPUAccelerator(AcceleratorBackend):
    # stateless — no __init__
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
        bundle_label_selector: Optional[Sequence[Dict[str, str]]] = None,
    ):
        resolved_selector = _resolve_bundle_label_selector(
            len(bundles), bundle_label_selector
        )
        return placement_group(
            bundles=bundles,
            strategy=strategy,
            name=name,
            bundle_label_selector=resolved_selector,
        )

    @property
    def requires_remote_initialization(self) -> bool:
        return False

    def get_remote_options(self, accelerator_type_str: str = None):
        return {}


class GPUAccelerator(AcceleratorBackend):
    # stateless — no __init__
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
        bundle_label_selector: Optional[Sequence[Dict[str, str]]] = None,
    ):
        resolved_selector = _resolve_bundle_label_selector(
            len(bundles), bundle_label_selector
        )
        return placement_group(
            bundles=bundles,
            strategy=strategy,
            name=name,
            bundle_label_selector=resolved_selector,
        )

    @property
    def requires_remote_initialization(self) -> bool:
        return True

    def get_remote_options(self, accelerator_type_str: str = None):
        options = {"num_gpus": 0.001}
        if accelerator_type_str:
            options["accelerator_type"] = accelerator_type_str
        return options


class TPUAccelerator(AcceleratorBackend):
    def __init__(self, config: TPUConfig):
        self._config = config
        self._slice_pg_wrapper = None

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
        version = get_tpu_version_from_type(accelerator_type_str)
        chips_per_host = get_chips_per_host(self._config.topology, version)

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
        bundle_label_selector: Optional[Sequence[Dict[str, str]]] = None,
    ) -> PlacementGroup:

        if not self._config.topology:
            clean_bundles, label_selector = prepare_tpu_bundles_and_label_selector(
                bundles, accelerator_type_str
            )
            resolved_selector = _resolve_bundle_label_selector(
                len(clean_bundles),
                bundle_label_selector,
                base_selectors=label_selector,
            )
            return placement_group(
                bundles=clean_bundles,
                strategy=strategy,
                name=name,
                bundle_label_selector=resolved_selector,
            )

        if not accelerator_type_str:
            raise ValueError(
                "accelerator_type must be provided for TPU slice provisioning."
            )

        # Validate all slice inputs before mutating or shutting down any existing slice PG.
        normalized_selector = normalize_bundle_label_selector(bundle_label_selector)
        normalized_topology = self._config.topology.strip().lower()
        version = get_tpu_version_from_type(accelerator_type_str)

        if not TPUAcceleratorManager.is_valid_tpu_accelerator_topology(
            tpu_accelerator_version=version,
            tpu_topology=normalized_topology,
        ):
            raise ValueError(
                f"Invalid accelerator topology: '{normalized_topology}' for "
                f"accelerator version: '{version}'"
            )

        if bundles:
            # Filter for bundles that actually specify TPU resources
            clean_bundles, _ = prepare_tpu_bundles_and_label_selector(bundles)
            tpu_bundles = [
                b for b in clean_bundles if _is_positive_number(b.get("TPU", 0))
            ]

            if not tpu_bundles:
                worker_bundle = {"TPU": 1}
            else:
                worker_bundle = tpu_bundles[0]

                # Ensure all TPU bundles are homogeneous
                if any(b != worker_bundle for b in tpu_bundles):
                    raise ValueError(
                        "Heterogeneous TPU bundles are not supported when `topology` is set. "
                        "A multi-host TPU slice requires homogeneous resource bundles across all workers. "
                        "Please use `bundle_per_worker` in `placement_group_config` to define uniform worker resources."
                    )
        else:
            # Default to 1 TPU per bundle.
            worker_bundle = {"TPU": 1}

        num_bundles, bundle_resources = get_tpu_worker_resources(
            topology=normalized_topology,
            accelerator_type=version,
            resources_per_worker=worker_bundle,
        )
        base_selectors = [
            {
                ray._raylet.RAY_NODE_TPU_TOPOLOGY_KEY: normalized_topology,
                ray._raylet.RAY_NODE_ACCELERATOR_TYPE_KEY: f"TPU-{version.upper()}",
            }
            for _ in range(num_bundles)
        ]
        resolved_selectors = _resolve_bundle_label_selector(
            num_bundles,
            normalized_selector,
            base_selectors=base_selectors,
        )
        validate_placement_group(
            bundles=[bundle_resources] * num_bundles,
            strategy="PACK",
            bundle_label_selector=resolved_selectors,
        )

        if self._slice_pg_wrapper is not None:
            logger.debug(
                "Existing TPU slice PG found. Shutting it down before creating a new one."
            )
            self.shutdown()

        if strategy != "PACK":
            logger.warning(
                f"Ignoring placement group strategy {strategy!r} for TPU topology "
                f"{self._config.topology!r}; SlicePlacementGroup manages per-slice placement."
            )

        self._slice_pg_wrapper = slice_placement_group(
            topology=self._config.topology,
            accelerator_version=version,
            resources_per_bundle=worker_bundle,
            name=name,
            bundle_label_selector=normalized_selector,
        )
        return self._slice_pg_wrapper.placement_group

    @property
    def requires_deferred_placement_group(self) -> bool:
        """If a TPU topology is specified, we defer PG creation so the replica can
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

    def shutdown(self):
        if self._slice_pg_wrapper is not None:
            try:
                logger.info("Shutting down TPU slice PG for server replica.")
                self._slice_pg_wrapper.shutdown()
            except Exception as e:
                logger.warning(f"Failed to shut down TPU slice PG: {e}")
            finally:
                self._slice_pg_wrapper = None
