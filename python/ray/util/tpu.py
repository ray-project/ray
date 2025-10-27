from typing import Optional

import ray
from ray._private.accelerators import TPUAcceleratorManager
from ray._private.accelerators.tpu import (
    VALID_TPU_TYPES,
    get_chips_per_host,
    reserve_tpu_slice,
)
from ray._private.client_mode_hook import client_mode_wrap
from ray.util.annotations import PublicAPI
from ray.util.placement_group import PlacementGroup, placement_group


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


@PublicAPI(stability="alpha")
def get_num_tpu_chips_on_node() -> int:
    """
    Return the number of TPU chips on the node.
    Returns:
        The total number of chips on the TPU node. Returns 0 if none are found.
    """
    return TPUAcceleratorManager.get_current_node_num_accelerators()


@PublicAPI(stability="alpha")
class SlicePlacementGroup:
    """
    A handle to a placement group reservation for a TPU slice.

    The following definitions are added for clarity:

    - Accelerator type: A string describing the accelerator type and version (e.g. TPU-V2, TPU-V6E).
    - Accelerator version: The accelerator generation only (e.g. v6e, v5p, v5litepod).
    - Pod type: The TPU accelerator version and the number of chips in a topology. (e.g. v6e-128, v5p-8).
    - Accelerator topology: The physical topology representing the structure (e.g. 2x2x2, 16x16).

        Args:
            topology: The TPU topology string (e.g. "2x2x2").
            accelerator_version: The TPU accelerator generation (e.g. "v6e", "v5p", "v4").
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

            num_slices: Number of TPU slices in the SlicePlacementGroup. Defaults to 1 when unspecified.

        Examples:

        .. testcode:: python
            :skipif: True

            import ray
            from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
            from ray.util.tpu import SlicePlacementGroup

            slice_handle = SlicePlacementGroup(topology="4x4", accelerator_version="v6e")
            slice_pg = slice_handle.placement_group
            ray.get(slice_pg.ready(), timeout=10)

            @ray.remote(num_cpus=0, resources={'TPU': 4})
            def spmd_task(world, rank):
                print(f"Current TPU is rank {rank} of {world}")

            tasks = [
                spmd_task.options(
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=slice_pg,
                    )
                ).remote(world=4, rank=i)
                for i in range(slice_handle.num_workers)
            ]

    """

    def __init__(
        self,
        topology: str,
        accelerator_version: str,
        # below are args related to PG
        strategy: str = "SPREAD",
        name: str = "",
        lifetime: Optional[str] = None,
        # default
        num_slices=1,
    ):
        self._topology = topology.strip().lower()
        self._accelerator_version = accelerator_version.strip().lower()
        self._num_slices = num_slices
        self._validate_tpu_config()

        # Reserve a TPU slice of the provided accelerator version and topology.
        self._placement_group = self._reserve_slice(
            strategy,
            name,
            lifetime,
        )

    def _accelerator_version_check(self, accelerator_version: str):
        if accelerator_version not in VALID_TPU_TYPES:
            raise ValueError(
                f"Invalid accelerator version: {accelerator_version}. Must be one of: {VALID_TPU_TYPES}"
            )

    def _validate_tpu_config(self):
        # Should validate topology and generation values, calculate and
        # set self._num_workers, and self._chips_per_host, and return a
        # ValueError if invalid.
        self._accelerator_version_check(self.accelerator_version)
        if not TPUAcceleratorManager.is_valid_tpu_accelerator_topology(
            tpu_accelerator_version=self.accelerator_version,
            tpu_topology=self._topology,
        ):
            raise ValueError(
                f"Invalid accelerator topology: '{self._topology}' for "
                f"accelerator version: '{self.accelerator_version}'"
            )

        total_chips = 1
        for value in self._topology.strip().lower().split("x"):
            total_chips *= int(value)

        self._chips_per_host = get_chips_per_host(
            self._topology, self.accelerator_version
        )
        self._num_workers_per_slice = total_chips // self._chips_per_host
        self._num_workers = self._num_workers_per_slice * self._num_slices

    def _reserve_slice(
        self,
        strategy: str = "SPREAD",
        name: str = "",
        lifetime: Optional[str] = None,
    ) -> PlacementGroup:
        """Performs the two-step scheduling to reserve a TPU slice."""
        bundle_label_selector = []
        bundles = []

        # Construct accelerator format for reserve_tpu_slice. e.g. From "v6e" to "TPU-V6E", "v5p" to "TPU-V5P".
        accelerator_type = "TPU-" + self.accelerator_version.upper()
        for _ in range(self.num_slices):
            # Reserving a slice is done through constructing num_workers bundles, each with a label selector for
            # the unique name of an available TPU slice.
            slice_name = reserve_tpu_slice(self._topology, accelerator_type)
            bundle_label_selector += [
                {ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY: slice_name}
            ] * self._num_workers_per_slice
            bundles += [{"TPU": self._chips_per_host}] * self._num_workers_per_slice

        pg = placement_group(
            bundles=bundles,
            strategy=strategy,
            name=name,
            lifetime=lifetime,
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
        """The total number of hosts in the SlicePlacementGroup."""
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
    def num_slices(self) -> int:
        """The number of TPU slices this SlicePlacementGroup spans."""
        return self._num_slices


@PublicAPI(stability="alpha")
@client_mode_wrap
def slice_placement_group(
    topology: str,
    accelerator_version: str,
    num_slices: int = 1,
    **kwargs,
) -> SlicePlacementGroup:
    """Asynchronously creates a PlacementGroup for a TPU slice.

    A slice placement group reserves num_slices TPU slice(s) and creates a placement
    group for scheduling tasks.

    Args:
        topology: The desired TPU pod topology (e.g. "4x4", "2x8").
        accelerator_version: The TPU accelerator generation, (e.g. "V4", "V5P", "V6E").
        num_slices: The number of tpu slices within the placement group
        **kwargs: Additional arguments for the placement group, such as 'name', 'lifetime', or 'strategy'.

    Returns:
        The handle for the created SlicePlacementGroup.
    """

    return SlicePlacementGroup(
        topology=topology,
        accelerator_version=accelerator_version,
        num_slices=num_slices,
        **kwargs,
    )
