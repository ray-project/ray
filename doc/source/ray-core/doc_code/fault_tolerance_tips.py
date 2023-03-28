# __return_ray_put_start__
import ray


# Non-fault tolerant version:
@ray.remote
def a():
    x_ref = ray.put(1)
    return x_ref


x_ref = ray.get(a.remote())
# Object x outlives its owner task A.
try:
    # If owner of x (i.e. the worker process running task A) dies,
    # the application can no longer get value of x.
    print(ray.get(x_ref))
except ray.exceptions.OwnerDiedError:
    pass
# __return_ray_put_end__


# __return_directly_start__
# Fault tolerant version:
@ray.remote
def a():
    # Here we return the value directly instead of calling ray.put() first.
    return 1


# The owner of x is the driver
# so x is accessible and can be auto recovered
# during the entire lifetime of the driver.
x_ref = a.remote()
print(ray.get(x_ref))
# __return_directly_end__


# __node_ip_resource_start__
@ray.remote
def b():
    return 1


# If the node with ip 127.0.0.3 fails while task b is running,
# Ray cannot retry the task on other nodes.
b.options(resources={"node:127.0.0.3": 1}).remote()
# __node_ip_resource_end__

# __node_affinity_scheduling_strategy_start__
# Prefer running on the particular node specified by node id
# but can also run on other nodes if the target node fails.
b.options(
    scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
        node_id=ray.get_runtime_context().get_node_id(), soft=True
    )
).remote()
# __node_affinity_scheduling_strategy_end__


# __manual_retry_start__
@ray.remote
class Actor:
    def read_only(self):
        import sys
        import random

        rand = random.random()
        if rand < 0.2:
            return 2 / 0
        elif rand < 0.3:
            sys.exit(1)

        return 2


actor = Actor.remote()
# Manually retry the actor task.
while True:
    try:
        print(ray.get(actor.read_only.remote()))
        break
    except ZeroDivisionError:
        pass
    except ray.exceptions.RayActorError:
        # Manually restart the actor
        actor = Actor.remote()
# __manual_retry_end__
