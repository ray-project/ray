from typing import List, Optional

from python.ray.util.scheduling_strategies import PlacementGroup

import ray
from ray._private.accelerators import TPUAcceleratorManager
from ray._private.accelerators.tpu import (
    VALID_TPU_TYPES,
    get_chips_per_host,
    reserve_tpu_slice,
)
from ray._private.client_mode_hook import client_mode_wrap
from ray.util.annotations import PublicAPI
from ray.util.placement_group import placement_group


@PublicAPI(stability="alpha")
def get_current_pod_name() -> Optional[str]:
    """
    Return the name of the TPU pod that the worker is a part of.

    Returns:
        The name of the TPU pod. Returns None if not part of a TPU pod.
    """
    tpu_name = TPUAcceleratorManager.get_current_node_tpu_name()
    if tpu_name == "":
        tpu_name = None
    return tpu_name


@PublicAPI(stability="alpha")
def get_current_pod_worker_count() -> Optional[int]:
    """
    Count the number of workers associated with the TPU pod that the worker belongs to.

    Returns:
        The total number of workers in the TPU pod. Returns None if the worker is not
        part of a TPU pod.
    """
    return TPUAcceleratorManager.get_num_workers_in_current_tpu_pod()


@PublicAPI(stablity="alpha")
def get_num_tpu_chips_on_node() -> int:
    """
    Return the number of TPU chips on the node.
    Returns:
        The total number of chips on the TPU node. Returns 0 if none are found.
    """
    return TPUAcceleratorManager.get_current_node_num_accelerators()


@PublicAPI
class SlicePlacementGroup:
    """A handle to a placement group reservation for a TPU slice.
    Added the following definitions for clarification:

    Accelerator type - accelerator with version. e.g. TPU-V2, TPU-V6E

    Accelerator size - accelerator version with the # of chips. e.g. v6e-128, v5p-8

    Accelerator topology - accelerator topology representing the structure. e.g. 2x2x2, 16x16

    Accelerator version - accelerator version only. e.g. v6e, v5p, v5litepod

        .. testcode::
            TODO

        .. testoutput::
            TODO

        Args:
            topology: The TPU topology string (e.g. "2x2x2").
            accelerator_version: The TPU accelerator version or generation (e.g. "v6", "v5p", "v4")
            strategy: PlacementGroup parameter. The strategy to create the placement group. Currently default to "SPREAD"

             - "PACK": Packs Bundles into as few nodes as possible.
             - "SPREAD": Places Bundles across distinct nodes as even as possible.
             - "STRICT_PACK": Packs Bundles into one node. The group is
               not allowed to span multiple nodes.
             - "STRICT_SPREAD": Packs Bundles across distinct nodes.

            lifetime: PlacementGroup parameter. Either `None`, which defaults to the placement group
                will fate share with its creator and will be deleted once its
                creator is dead, or "detached", which means the placement group
                will live as a global object independent of the creator.
            _soft_target_node_id: (Private, Experimental) PlacementGroup parameter. Soft hint where bundles of
                this placement group should be placed.
                The target node is specified by it's hex ID.
                If the target node has no available resources or died,
                bundles can be placed elsewhere.
                This currently only works with STRICT_PACK pg.
            num_slices: Number of TPU slices in the TPU placement group. Currently default to 1.
    """

    def __init__(
        self,
        topology: str,
        accelerator_version: str,
        # below are args related to PG
        strategy: str = "SPREAD",
        name: str = "",
        lifetime: Optional[str] = None,
        _soft_target_node_id: Optional[str] = None,
        # default
        num_slices=1,
    ):
        self._topology = topology.strip().lower()
        self._accelerator_version = accelerator_version.strip().lower()
        self._num_slices = num_slices

        self._validate_tpu_config()

        # implement the logic from Ray Train to reserve the PG using bundle selectors
        self._placement_group = self._reserve_slice(
            strategy,
            name,
            lifetime,
            _soft_target_node_id,
        )

    def _accelerator_version_check(accelerator_version: str):
        if accelerator_version not in VALID_TPU_TYPES:
            raise ValueError(
                f"Invalid accelerator version: {accelerator_version}. Must be one of: {VALID_TPU_TYPES}"
            )

    def _validate_tpu_config(self):
        # Should validate topology and generation values, calculate and
        # set self._num_workers, and self._chips_per_host, and return a
        # ValueError if invalid.
        total_chips = 1
        for value in self._topology.strip().lower().split("x"):
            total_chips *= int(value)

        self._accelerator_version_check(self.accelerator_version)
        if not TPUAcceleratorManager.is_valid_tpu_accelerator_topology(
            self.accelerator_version
        ):
            raise ValueError("Invalid accelerator topology")

        self._chips_per_host = get_chips_per_host(
            self._topology, self.accelerator_version
        )
        num_worker_per_slice = total_chips // self._chips_per_host
        self._num_workers = num_worker_per_slice * self._num_slices

    @property
    def _reserve_slice(
        self,
        # below are args related to PG
        strategy: str = "SPREAD",
        name: str = "",
        lifetime: Optional[str] = None,
        _soft_target_node_id: Optional[str] = None,
    ) -> PlacementGroup:
        """Performs the two-step scheduling to reserve a TPU slice."""
        bundle_label_selector = List()
        bundles = List()

        # construct accelerator format for reserve_tpu_slice. e.g. From "v6e" to "TPU-V6E", "v5p" to "TPU-V5P"
        accelerator_type = "TPU-" + self.accelerator_version.capitalize()
        for _ in range(self.num_slices):
            slice_name = reserve_tpu_slice(self._topology, accelerator_type)
            bundle_label_selector.append(
                {ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY: slice_name}
            )
            bundles.append({"TPU": self._num_workers})

        pg = placement_group(
            bundles=bundles,
            strategy=strategy,
            name=name,
            lifetime=lifetime,
            _soft_target_node_id=_soft_target_node_id,
            bundle_label_selector=bundle_label_selector,
        )

        return pg

    @property
    def placement_group(self) -> PlacementGroup:
        """The underlying PlacementGroup object."""
        return self._placement_group

    @property
    def chips_per_host(self) -> int:
        """The number of chips per host for this TPU slice."""
        # This is the same value as resources per worker for TPU.
        return self._chips_per_host

    @property
    def num_workers(self) -> int:
        """The number of hosts in the TPU slice."""
        return self._num_workers

    @property
    def topology(self) -> str:
        """The physical topology of the TPU slice."""
        return self._topology

    @property
    def accelerator_version(self) -> str:
        """The TPU accelerator type of the slice."""
        return self._accelerator_version

    @property
    def num_slices(self) -> str:
        """The number of TPU slices this SlicePlacementGroup spans."""
        return self._num_slices


@PublicAPI
@client_mode_wrap
def slice_placement_group(
    topology: str,
    accelerator_version: str,
    **kwargs,
) -> SlicePlacementGroup:
    """Asynchronously creates a PlacementGroup for a TPU slice.

            A slice placement group reserves a TPU slice and creates a placement
    group for scheduling tasks.

            Args:
    topology: The desired TPU pod topology, e.g., "4x4", "2x8".
    accelerator_version: The TPU accelerator_version, e.g., "V4", "V5P", "V6E".
    **kwargs: Additional arguments for the placement group, such as 'name',
                      'lifetime', or 'strategy'.

            Returns:
                    The handle for the created SlicePlacementGroup.
    """

    return SlicePlacementGroup(
        topology=topology, accelerator_version=accelerator_version, **kwargs
    )


@PublicAPI
@client_mode_wrap
def multi_slice_placement_group(
    num_slices: int,
    topology: str,
    accelerator_version: str,
    **kwargs,
) -> SlicePlacementGroup:
    """Asynchronously create num_slices of SlicePlacementGroups

        A slice placement group that reserves nums_slices of TPU slices and creates
    a placement group for scheduling tasks

        Args:
    num_slices: The number of tpu slices within the placement group
    topology: The desired TPU pod topology, e.g., "4x4", "2x8".
    accelerator_version: The TPU accelerator_version, e.g., "V4", "V5P", "V6E".
    **kwargs: Additional arguments for the placement group, such as 'name',
                      'lifetime', or 'strategy'.

        Returns:
            The handle for the SlicePlacementGroup
    """

    return SlicePlacementGroup(
        num_slices=num_slices,
        topology=topology,
        accelerator_version=accelerator_version,
        **kwargs,
    )
