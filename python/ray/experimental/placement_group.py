import ray


def placement_group(name, strategy, bundles):
    worker = ray.worker.global_worker
    placement_group_id = worker.core_worker.create_placement_group(
        name, bundles, strategy)
    return placement_group_id
