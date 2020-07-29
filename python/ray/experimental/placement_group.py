import ray
from typing import (List, Dict)


def placement_group(bundles: List[Dict[str, float]],
                    strategy: str = "PACK",
                    name: str = None):
    """
    Create a placement group.

    This method is the api to create placement group.

    Args:
        bundles: A list of bundles which represent the resources needed.
        strategy: The strategy to create the placement group.
            There are two build-in strategies for the time begin.
            PACK: Packs Bundles close together inside processes or nodes as
            tight as possible.
            SPREAD: Places Bundles across distinct nodes or processes as even
            as possible.
        name: The name of the placement group.
    """
    worker = ray.worker.global_worker
    placement_group_id = worker.core_worker.create_placement_group(
        name, bundles, strategy)
    return placement_group_id
