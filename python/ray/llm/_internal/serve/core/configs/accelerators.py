from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field
from typing_extensions import Annotated

import ray.util.accelerators.accelerators as accelerators
from ray.llm._internal.serve.observability.logging import get_logger
from ray.util.placement_group import PlacementGroup, placement_group
from ray.util.tpu import get_tpu_version_from_type, slice_placement_group

logger = get_logger(__name__)

AcceleratorType = Enum("AcceleratorType", vars(accelerators))

# Set of TPU string values from Ray's known accelerators.
TPU_ACCELERATOR_VALUES = {
    member.value
    for name, member in AcceleratorType.__members__.items()
    if name.startswith("GOOGLE_TPU")
}


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
        ray_accelerator_type: Optional[str] = None,
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
        self, *, num_devices: int, ray_accelerator_type: Optional[str] = None
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
    # stateless — no __init__
    def default_bundles(
        self, *, num_devices: int, ray_accelerator_type: Optional[str] = None
    ):
        bundle = {"GPU": 1}
        if ray_accelerator_type:
            bundle[ray_accelerator_type] = 0.001
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
    def __init__(self, config: TPUConfig):
        self._config = config
        self._slice_pg_wrapper = None

    def default_bundles(
        self, *, num_devices: int, ray_accelerator_type: Optional[str] = None
    ):
        bundle = {"TPU": 1}
        if ray_accelerator_type:
            bundle[ray_accelerator_type] = 0.001
        return [bundle.copy() for _ in range(num_devices)]

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

        version = get_tpu_version_from_type(accelerator_type_str)

        if bundles:
            # Filter for bundles that actually specify TPU resources
            tpu_bundles = [b for b in bundles if b.get("TPU", 0) > 0]

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

        self._slice_pg_wrapper = slice_placement_group(
            topology=self._config.topology,
            accelerator_version=version,
            resources_per_bundle=worker_bundle,
            strategy=strategy,
            name=name,
        )
        return self._slice_pg_wrapper.placement_group

    @property
    def requires_remote_initialization(self) -> bool:
        return True

    def get_remote_options(self, accelerator_type_str: str = None):
        # TPUs use custom resource strings rather than a native kwarg
        options: Dict[str, Any] = {"resources": {"TPU": 0.001}}

        if accelerator_type_str:
            options["accelerator_type"] = accelerator_type_str
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

    def __del__(self):
        """Ensure placement groups are cleaned up when this backend is garbage collected."""
        self.shutdown()
