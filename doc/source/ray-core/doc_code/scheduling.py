import ray

ray.init(num_cpus=64)


# __default_scheduling_strategy_start__
@ray.remote
def func():
    return 1


@ray.remote(num_cpus=1)
class Actor:
    pass


# If unspecified, "DEFAULT" scheduling strategy is used.
func.remote()
actor = Actor.remote()
# Explicitly set scheduling strategy to "DEFAULT".
func.options(scheduling_strategy="DEFAULT").remote()
actor = Actor.options(scheduling_strategy="DEFAULT").remote()

# Zero-CPU (and no other resources) actors are randomly assigned to nodes.
actor = Actor.options(num_cpus=0).remote()
# __default_scheduling_strategy_end__


# __spread_scheduling_strategy_start__
@ray.remote(scheduling_strategy="SPREAD")
def spread_func():
    return 2


@ray.remote(num_cpus=1)
class SpreadActor:
    pass


# Spread tasks across the cluster.
[spread_func.remote() for _ in range(10)]
# Spread actors across the cluster.
actors = [SpreadActor.options(scheduling_strategy="SPREAD").remote() for _ in range(10)]
# __spread_scheduling_strategy_end__


# __node_affinity_scheduling_strategy_start__
@ray.remote
def node_affinity_func():
    return ray.get_runtime_context().node_id


@ray.remote(num_cpus=1)
class NodeAffinityActor:
    pass


# Only run the task on the local node.
node_affinity_func.options(
    scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
        node_id=ray.get_runtime_context().node_id,
        soft=False,
    )
).remote()

# Run the two node_affinity_func tasks on the same node if possible.
node_affinity_func.options(
    scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
        node_id=ray.get(node_affinity_func.remote()),
        soft=True,
    )
).remote()

# Only run the actor on the local node.
actor = NodeAffinityActor.options(
    scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
        node_id=ray.get_runtime_context().node_id,
        soft=False,
    )
).remote()
# __node_affinity_scheduling_strategy_end__


# __locality_aware_scheduling_start__
@ray.remote
def large_object_func():
    # Large object is stored in the local object store
    # and available in the distributed memory,
    # instead of returning inline directly to the caller.
    return [1] * (1024 * 1024)


@ray.remote
def small_object_func():
    # Small object is returned inline directly to the caller,
    # instead of storing in the distributed memory.
    return [1]


@ray.remote
def consume_func(data):
    return len(data)


large_object = large_object_func.remote()
small_object = small_object_func.remote()

# Ray will try to run consume_func on the same node
# where large_object_func runs.
consume_func.remote(large_object)

# Ray will try to spread consume_func across the entire cluster
# instead of only running on the node where large_object_func runs.
[
    consume_func.options(scheduling_strategy="SPREAD").remote(large_object)
    for i in range(10)
]

# Ray won't consider locality for scheduling consume_func
# since the argument is small and will be sent to the worker node inline directly.
consume_func.remote(small_object)
# __locality_aware_scheduling_end__
