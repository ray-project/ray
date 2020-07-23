import ray


def placement_group(bundles, strategy: str = "PACK", name: str = None):
    """
    placement_group.

    This method is the api to create placement group.

    Args:
    bundles: a list of the bundle which represet the resource needed. 
    strategy: the strategy of the placement group.
        Now it supports PACK and SPREAD.
    name: the name of the placement group.

    """
    worker = ray.worker.global_worker
    placement_group_id = worker.core_worker.create_placement_group(
        name, bundles, strategy)
    return placement_group_id
