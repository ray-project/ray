# This workload tests running PBT

import ray
from ray.tune import run_experiments
from ray.tune.schedulers import PopulationBasedTraining
from ray.cluster_utils import Cluster
from ray.tune.utils.release_test_util import ProgressCallback

num_redis_shards = 5
redis_max_memory = 10 ** 8
object_store_memory = 10 ** 8
num_nodes = 3

message = (
    "Make sure there is enough memory on this machine to run this "
    "workload. We divide the system memory by 2 to provide a buffer."
)
assert (
    num_nodes * object_store_memory + num_redis_shards * redis_max_memory
    < ray._private.utils.get_system_memory() / 2
), message

# Simulate a cluster on one machine.

cluster = Cluster()
for i in range(num_nodes):
    cluster.add_node(
        redis_port=6379 if i == 0 else None,
        num_redis_shards=num_redis_shards if i == 0 else None,
        num_cpus=10,
        num_gpus=0,
        resources={str(i): 2},
        object_store_memory=object_store_memory,
        redis_max_memory=redis_max_memory,
        dashboard_host="0.0.0.0",
    )
ray.init(address=cluster.address)

# Run the workload.

pbt = PopulationBasedTraining(
    time_attr="training_iteration",
    metric="episode_reward_mean",
    mode="max",
    perturbation_interval=10,
    hyperparam_mutations={
        "lr": [0.1, 0.01, 0.001, 0.0001],
    },
)

run_experiments(
    {
        "pbt_test": {
            "run": "PG",
            "env": "CartPole-v0",
            "num_samples": 8,
            "config": {
                "lr": 0.01,
            },
        }
    },
    callbacks=[ProgressCallback()],
    scheduler=pbt,
    verbose=False,
)
