from typing import (List, Dict)

import ray
from ray._raylet import PlacementGroupID, ObjectRef


class PlacementGroup:
    """A handle to a placement group."""

    @staticmethod
    def empty():
        return PlacementGroup(PlacementGroupID.nil(), [])

    def __init__(self, id: PlacementGroupID, bundles: List[Dict[str, float]]):
        self.id = id
        self.bundles = bundles

    def ready(self) -> ObjectRef:
        """Returns an ObjectRef to check ready status.

        This API runs a small dummy task to wait for placement group creation.
        It is compatible to ray.get and ray.wait.

        Example:

        >>> pg = placement_group([{"CPU": 1}])
            ray.get(pg.ready())

        >>> pg = placement_group([{"CPU": 1}])
            ray.wait([pg.ready()], timeout=0)
        """
        worker = ray.worker.global_worker
        worker.check_connected()

        @ray.remote(num_cpus=0, max_calls=0)
        def bundle_reservation_check(placement_group):
            return placement_group

        assert len(self.bundles) != 0, (
            "ready() cannot be called on placement group object with a "
            f"bundle length == 0, current bundle length: {len(self.bundles)}")

        # Select the first bundle to schedule a dummy task.
        # Since the placement group creation will be atomic, it is sufficient
        # to schedule a single task.
        bundle_index = 0
        bundle = self.bundles[bundle_index]

        resource_name, value = self._get_none_zero_resource(bundle)
        num_cpus = 0
        num_gpus = 0
        resources = None
        if resource_name == "CPU":
            num_cpus = value
        elif resource_name == "GPU":
            num_gpus = value
        else:
            resources[resource_name] = value

        return bundle_reservation_check.options(
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            placement_group=self,
            placement_group_bundle_index=bundle_index,
            resources=resources).remote(self)

    @property
    def bundle_specs(self) -> List[Dict]:
        """List[Dict]: Return bundles belonging to this placement group."""
        return self.bundles

    @property
    def bundle_count(self):
        return len(self.bundles)

    def _get_none_zero_resource(self, bundle: List[Dict]):
        for key, value in bundle.items():
            if value > 0:
                value = min(value, 0.001)
                return key, value
        assert False, "This code should be unreachable."


def placement_group(bundles: List[Dict[str, float]],
                    strategy: str = "PACK",
                    name: str = "unnamed_group") -> PlacementGroup:
    """Asynchronously creates a PlacementGroup.

    Args:
        bundles(List[Dict]): A list of bundles which
            represent the resources requirements.
        strategy(str): The strategy to create the placement group.

         - "PACK": Packs Bundles into as few nodes as possible.
         - "SPREAD": Places Bundles across distinct nodes as even as possible.
         - "STRICT_PACK": Packs Bundles into one node. The group is
           not allowed to span multiple nodes.
         - "STRICT_SPREAD": Packs Bundles across distinct nodes.

        name(str): The name of the placement group.

    Return:
        PlacementGroup: Placement group object.
    """
    worker = ray.worker.global_worker
    worker.check_connected()

    if not isinstance(bundles, list):
        raise ValueError(
            "The type of bundles must be list, got {}".format(bundles))

    # Validate bundles
    for bundle in bundles:
        if (len(bundle) == 0 or all(resource_value == 0
                                    for resource_value in bundle.values())):
            raise ValueError(
                "Bundles cannot be an empty dictionary or "
                f"resources with only 0 values. Bundles: {bundles}")

    placement_group_id = worker.core_worker.create_placement_group(
        name, bundles, strategy)

    return PlacementGroup(placement_group_id, bundles)


def remove_placement_group(placement_group: PlacementGroup):
    """Asynchronously remove placement group.

    Args:
        placement_group (PlacementGroup): The placement group to delete.
    """
    assert placement_group is not None
    worker = ray.worker.global_worker
    worker.check_connected()

    worker.core_worker.remove_placement_group(placement_group.id)


def placement_group_table(placement_group: PlacementGroup) -> dict:
    """Get the state of the placement group from GCS.

    Args:
        placement_group (PlacementGroup): placement group to see
            states.
    """
    assert placement_group is not None
    worker = ray.worker.global_worker
    worker.check_connected()
    return ray.state.state.placement_group_table(placement_group.id)


def check_placement_group_index(placement_group: PlacementGroup,
                                bundle_index: int):
    assert placement_group is not None
    if placement_group.id.is_nil():
        if bundle_index != -1:
            raise ValueError("If placement group is not set, "
                             "the value of bundle index must be -1.")
    elif bundle_index >= placement_group.bundle_count \
            or bundle_index < -1:
        raise ValueError(f"placement group bundle index {bundle_index} "
                         f"is invalid. Valid placement group indexes: "
                         f"0-{placement_group.bundle_count}")
