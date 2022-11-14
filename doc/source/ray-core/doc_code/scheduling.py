import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


@ray.remote
def default_function():
    return 1


# If unspecified, "DEFAULT" scheduling strategy is used.
default_function.remote()

# Explicitly set scheduling strategy to "DEFAULT".
default_function.options(scheduling_strategy="DEFAULT").remote()


@ray.remote(scheduling_strategy="SPREAD")
def spread_function():
    return 2


# Spread tasks across the cluster.
[spread_function.remote() for i in range(100)]


@ray.remote
def node_affinity_function():
    return ray.get_runtime_context().node_id


# Only run the task on the local node.
node_affinity_function.options(
    scheduling_strategy=NodeAffinitySchedulingStrategy(
        node_id=ray.get_runtime_context().node_id,
        soft=False,
    )
).remote()

# Run the two node_affinity_function tasks on the same node if possible.
node_affinity_function.options(
    scheduling_strategy=NodeAffinitySchedulingStrategy(
        node_id=ray.get(node_affinity_function.remote()),
        soft=True,
    )
).remote()
