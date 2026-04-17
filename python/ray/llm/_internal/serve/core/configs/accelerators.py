import copy
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import ray.util.accelerators.accelerators as accelerators
from ray.llm._internal.serve.observability.logging import get_logger
from ray.util.placement_group import PlacementGroup, placement_group

AcceleratorType = Enum("AcceleratorType", vars(accelerators))

logger = get_logger(__name__)


# Set of TPU string values from Ray's known accelerators.
TPU_ACCELERATOR_VALUES = {
    member.value
    for name, member in AcceleratorType.__members__.items()
    if name.startswith("GOOGLE_TPU")
}


def _compute_use_gpu(
    use_cpu: Optional[bool],
    placement_group_config: Optional[Dict[str, Any]],
    accelerator_type: Optional[Union[str, AcceleratorType]] = None,
) -> bool:
    """Returns True if the configuration resolves to GPU usage.

    Priority order:
    1. Explicit use_cpu flag
    2. placement_group_config GPU bundles
    3. Default to True if not explicitly CPU or TPU.
    """
    # If explicitly requesting CPU, it's not GPU.
    if isinstance(use_cpu, bool) and use_cpu:
        return False

    # Check placement_group_config for explicit GPU specification
    if placement_group_config:
        bundle_per_worker = placement_group_config.get("bundle_per_worker")
        if bundle_per_worker:
            return bundle_per_worker.get("GPU", 0) > 0

        # Check bundles list (empty list → no GPUs → CPU-only)
        bundles = placement_group_config.get("bundles")
        if bundles is not None:
            return any(bundle.get("GPU", 0) > 0 for bundle in bundles)

    # If the user explicitly requested a TPU, it's not a GPU.
    if accelerator_type:
        accel_str = getattr(accelerator_type, "value", str(accelerator_type))
        if accel_str in TPU_ACCELERATOR_VALUES:
            return False

    # All remaining accelerator types are GPU-capable.
    return True


def _compute_use_tpu(
    use_cpu: Optional[bool],
    placement_group_config: Optional[Dict[str, Any]],
    accelerator_type: Optional[Union[str, AcceleratorType]] = None,
) -> bool:
    """Returns True if the configuration resolves to TPU usage."""
    if isinstance(use_cpu, bool) and use_cpu:
        return False

    if placement_group_config:
        bundle_per_worker = placement_group_config.get("bundle_per_worker")
        if bundle_per_worker:
            return bundle_per_worker.get("TPU", 0) > 0

        bundles = placement_group_config.get("bundles")
        if bundles is not None:
            return any(bundle.get("TPU", 0) > 0 for bundle in bundles)

    if not accelerator_type:
        return False

    accel_str = getattr(accelerator_type, "value", str(accelerator_type))
    return accel_str in TPU_ACCELERATOR_VALUES


class AcceleratorBackend(ABC):
    """Abstract base class for hardware-specific configuration strategies."""

    def __init__(self, config: Any):
        self.config = config

    @abstractmethod
    def get_placement_bundles(self) -> List[Dict[str, float]]:
        """Generates the default placement group bundles for the hardware."""
        pass

    @abstractmethod
    def create_placement_group(self, name: str) -> PlacementGroup:
        """Creates the appropriate Ray placement group."""
        pass


class CPUAccelerator(AcceleratorBackend):
    def get_placement_bundles(self) -> List[Dict[str, float]]:
        bundle = {"CPU": 1}
        return [copy.deepcopy(bundle) for _ in range(self.config.num_devices)]

    def create_placement_group(self, name: str) -> PlacementGroup:
        return placement_group(
            bundles=self.config.placement_bundles,
            strategy=self.config.placement_strategy,
            name=name,
        )


class GPUAccelerator(AcceleratorBackend):
    def get_placement_bundles(self) -> List[Dict[str, float]]:
        bundle = {"GPU": 1}
        if self.config.accelerator_type:
            bundle[self.config.ray_accelerator_type()] = 0.001
        return [copy.deepcopy(bundle) for _ in range(self.config.num_devices)]

    def create_placement_group(self, name: str) -> PlacementGroup:
        return placement_group(
            bundles=self.config.placement_bundles,
            strategy=self.config.placement_strategy,
            name=name,
        )


class TPUAccelerator(AcceleratorBackend):
    def get_placement_bundles(self) -> List[Dict[str, float]]:
        bundle = {"TPU": 1}
        if self.config.accelerator_type:
            bundle[self.config.ray_accelerator_type()] = 0.001
        return [copy.deepcopy(bundle) for _ in range(self.config.num_devices)]

    def create_placement_group(self, name: str) -> PlacementGroup:
        if not self.config.topology:
            return placement_group(
                bundles=self.config.placement_bundles,
                strategy=self.config.placement_strategy,
                name=name,
            )

        from ray.util.tpu import get_tpu_version_from_type, slice_placement_group

        accel_str = (
            self.config.accelerator_type.value
            if hasattr(self.config.accelerator_type, "value")
            else str(self.config.accelerator_type)
        )
        version = get_tpu_version_from_type(accel_str)
        topology = self.config.topology

        logger.info(
            f"Provisioning TPU Slice Placement Group: {version} with topology {topology}"
        )

        placement_bundles = self.config.placement_bundles
        if placement_bundles:
            # 1. Filter out CPU-only driver bundles
            tpu_bundles = [b for b in placement_bundles if b.get("TPU", 0) > 0]

            if not tpu_bundles:
                worker_bundle = {"TPU": 1}
            else:
                # 2. Safely grab the first actual TPU bundle
                worker_bundle = tpu_bundles[0]

                # 3. Raise an error ONLY if the TPU bundles are heterogeneous (fixes the bug!)
                if any(b != worker_bundle for b in tpu_bundles):
                    raise ValueError(
                        "Heterogeneous TPU bundles are not supported when `topology` is set. "
                        "A multi-host TPU slice requires homogeneous resource bundles across all workers. "
                        "Please use `bundle_per_worker` in `placement_group_config` to define uniform worker resources."
                    )
        else:
            worker_bundle = {"TPU": 1}

        slice_pg_wrapper = slice_placement_group(
            topology=topology,
            accelerator_version=version,
            resources_per_bundle=worker_bundle,
            strategy=self.config.placement_strategy,
            name=name,
        )
        self.config._tpu_slice_pg_wrapper = slice_pg_wrapper
        return slice_pg_wrapper.placement_group
