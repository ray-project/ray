# __child_capture_pg_start__
import ray
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

ray.init(num_cpus=2)

# Create a placement group.
pg = placement_group([{"CPU": 2}])
ray.get(pg.ready())


@ray.remote(num_cpus=1)
def child():
    import time

    time.sleep(5)


@ray.remote(num_cpus=1)
def parent():
    # The child task is scheduled to the same placement group as its parent,
    # although it didn't specify the PlacementGroupSchedulingStrategy.
    ray.get(child.remote())


# Since the child and parent use 1 CPU each, the placement group
# bundle {"CPU": 2} is fully occupied.
ray.get(
    parent.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg, placement_group_capture_child_tasks=True
        )
    ).remote()
)
# __child_capture_pg_end__


# __child_capture_disable_pg_start__
@ray.remote
def parent():
    # In this case, the child task isn't
    # scheduled with the parent's placement group.
    ray.get(
        child.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=None)
        ).remote()
    )


# This times out because we cannot schedule the child task.
# The cluster has {"CPU": 2}, and both of them are reserved by
# the placement group with a bundle {"CPU": 2}. Since the child shouldn't
# be scheduled within this placement group, it cannot be scheduled because
# there's no available CPU resources.
try:
    ray.get(
        parent.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_capture_child_tasks=True
            )
        ).remote(),
        timeout=5,
    )
except Exception as e:
    print("Couldn't create a child task!")
    print(e)
# __child_capture_disable_pg_end__
