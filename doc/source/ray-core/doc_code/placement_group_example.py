# __create_pg_start__
from pprint import pprint
import time

# Import placement group APIs.
from ray.util.placement_group import (
    placement_group,
    placement_group_table,
    remove_placement_group,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

# Initialize Ray.
import ray

# Create a single node Ray cluster with 2 CPUs and 2 GPUs.
ray.init(num_cpus=2, num_gpus=2)

# Reserve a placement group of 1 bundle that reserves 1 CPU and 1 GPU.
pg = placement_group([{"CPU": 1, "GPU": 1}])
# __create_pg_end__

# __ready_pg_start__
# Wait until placement group is created.
ray.get(pg.ready(), timeout=10)

# You can also use ray.wait.
ready, unready = ray.wait([pg.ready()], timeout=10)

# You can look at placement group states using this API.
print(placement_group_table(pg))
# __ready_pg_end__

# __create_pg_failed_start__
# Cannot create this placement group because we
# cannot create a {"GPU": 2} bundle.
pending_pg = placement_group([{"CPU": 1}, {"GPU": 2}])
# This raises the timeout exception!
try:
    ray.get(pending_pg.ready(), timeout=5)
except Exception as e:
    print(
        "Cannot create a placement group because "
        "{'GPU': 2} bundle cannot be created."
    )
    print(e)
# __create_pg_failed_end__


# __schedule_pg_start__
@ray.remote(num_cpus=1)
class Actor:
    def __init__(self):
        pass

    def ready(self):
        pass


# Create an actor to a placement group.
actor = Actor.options(
    scheduling_strategy=PlacementGroupSchedulingStrategy(
        placement_group=pg,
    )
).remote()

# Verify the actor is scheduled.
ray.get(actor.ready.remote(), timeout=10)
# __schedule_pg_end__


# __schedule_pg_3_start__
@ray.remote(num_cpus=0, num_gpus=1)
class Actor:
    def __init__(self):
        pass

    def ready(self):
        pass


# Create a GPU actor on the first bundle of index 0.
actor2 = Actor.options(
    scheduling_strategy=PlacementGroupSchedulingStrategy(
        placement_group=pg,
        placement_group_bundle_index=0,
    )
).remote()

# Verify that the GPU actor is scheduled.
ray.get(actor2.ready.remote(), timeout=10)
# __schedule_pg_3_end__

# __remove_pg_start__
# This API is asynchronous.
remove_placement_group(pg)

# Wait until placement group is killed.
time.sleep(1)
# Check that the placement group has died.
pprint(placement_group_table(pg))

"""
{'bundles': {0: {'GPU': 1.0}, 1: {'CPU': 1.0}},
'name': 'unnamed_group',
'placement_group_id': '40816b6ad474a6942b0edb45809b39c3',
'state': 'REMOVED',
'strategy': 'PACK'}
"""
# __remove_pg_end__

# __strategy_pg_start__
# Reserve a placement group of 2 bundles
# that have to be packed on the same node.
pg = placement_group([{"CPU": 1}, {"GPU": 1}], strategy="PACK")
# __strategy_pg_end__

remove_placement_group(pg)

# __detached_pg_start__
# driver_1.py
# Create a detached placement group that survives even after
# the job terminates.
pg = placement_group([{"CPU": 1}], lifetime="detached", name="global_name")
ray.get(pg.ready())
# __detached_pg_end__
remove_placement_group(pg)


# __get_pg_start__
# first_driver.py
# Create a placement group with a unique name within a namespace.
# Start Ray or connect to a Ray cluster using: ray.init(namespace="pg_namespace")
pg = placement_group([{"CPU": 1}], name="pg_name")
ray.get(pg.ready())

# second_driver.py
# Retrieve a placement group with a unique name within a namespace.
# Start Ray or connect to a Ray cluster using: ray.init(namespace="pg_namespace")
pg = ray.util.get_placement_group("pg_name")
# __get_pg_end__
