from typing import (List, Dict)

import ray


class PlacementGroup:
    """A handle to a placement group.
    """

    def __init__(self, id, bundle_count):
        self.id = id
        self.bundle_count = bundle_count


def placement_group(bundles: List[Dict[str, float]],
                    strategy: str = "PACK",
                    name: str = "unnamed_group"):
    """
    Create a placement group.

    This method is the api to create placement group.

    Args:
        bundles: A list of bundles which represent the resources needed.
        strategy: The strategy to create the placement group.
            PACK: Packs Bundles into as few nodes as possible.
            SPREAD: Places Bundles across distinct nodes as even as possible.
            STRICT_PACK: Packs Bundles into one node.
            STRICT_SPREAD: Packs Bundles across distinct nodes.
            The group is not allowed to span multiple nodes.
        name: The name of the placement group.
    """
    worker = ray.worker.global_worker
    worker.check_connected()

    if not isinstance(bundles, list):
        raise ValueError(
            "The type of bundles must be list, got {}".format(bundles))

    placement_group_id = worker.core_worker.create_placement_group(
        name, bundles, strategy)

    return PlacementGroup(placement_group_id, len(bundles))


def remove_placement_group(placement_group):
    assert placement_group is not None
    worker = ray.worker.global_worker
    worker.check_connected()

    worker.core_worker.remove_placement_group(placement_group.id)


def placement_group_table(placement_group):
    assert placement_group is not None
    worker = ray.worker.global_worker
    worker.check_connected()
    return ray.state.state.placement_group_table(placement_group.id)


def check_placement_group_index(placement_group, bundle_index):
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
