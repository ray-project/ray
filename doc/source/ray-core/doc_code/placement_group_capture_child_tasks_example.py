import ray
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

ray.init(num_cpus=4)

# Create a placement group with the SPREAD strategy.
pg = placement_group([{"CPU": 2}, {"CPU": 2}], strategy="SPREAD")
ray.get(pg.ready())


@ray.remote(num_cpus=1)
def child():
    pass


@ray.remote(num_cpus=1)
def parent():
    # The child task is scheduled with the same placement group as its parent
    # although child.options(
    #     scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
    # ).remote() wasn't called if placement_group_capture_child_tasks is set to True.
    ray.get(child.remote())


ray.get(
    parent.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg, placement_group_capture_child_tasks=True
        )
    ).remote()
)
