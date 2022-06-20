import ray
from ray.util.placement_group import (
    placement_group,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

# Two "CPU"s are available.
ray.init(num_cpus=2)

# Create a placement group.
pg = placement_group([{"CPU": 2}])
ray.get(pg.ready())


# Now, 2 CPUs are not available anymore because
# they are pre-reserved by the placement group.
@ray.remote(num_cpus=2)
def f():
    return True


# Won't be scheduled because there are no 2 cpus.
f.remote()

# Will be scheduled because 2 cpus are reserved by the placement group.
f.options(
    scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
).remote()
